package com.convertlab.identity

import java.time.LocalDateTime
import java.util.UUID

import com.convertlab.common.{DataUtils, Identity}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteIdentityDataToKafka {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("WriteIdentityDataToKafka")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val originDs: DataFrame = spark.sql("select * from identity_ro")
    originDs.createOrReplaceTempView("identity_temp_view")
    for (i <- 1 to 4) {
      val uuid = UUID.randomUUID().toString.substring(0, 4)
      val identitySchema = Identity.getIdentitySchema
      val identityRdd = sc.parallelize(0 until 300000, 100).map(i => DataUtils.fillData(identitySchema, i, identityType = uuid))

      val updateDs = spark.createDataFrame(identityRdd, identitySchema)

      updateDs
        .select(to_json(struct("*")).as("value"))
        .selectExpr(s"CAST('${UUID.randomUUID().toString}' AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "b-1.dev-hudipoc-ms.vto656.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092,b-2.dev-hudipoc-ms.vto656.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092")
        .option("topic", "kafka_hudi_identity")
        .save()

      println(LocalDateTime.now() + s"===== finish write data into kafka, index $i")
    }

    spark.stop()

  }
}