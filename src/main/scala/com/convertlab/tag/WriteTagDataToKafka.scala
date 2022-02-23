package com.convertlab.tag

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.util.UUID

import scala.util.Random

object WriteTagDataToKafka {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("WriteTagDataToKafka")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val originDs: DataFrame = spark.sql("select * from tag_ro")
    originDs.createOrReplaceTempView("tag_temp_view")
    val changeValue = Random.nextInt(100)
    println(s"change value to $changeValue")
    for (i <- 1 to 2) {
      println(LocalDateTime.now() + s"===== start write data into kafka, index $i")
      val beginIndex = i * 100000
      val endIndex = (i + 1) * 100000
      val updateDs = spark.sql(s"select * from tag_temp_view where _id > $beginIndex and _id < $endIndex")
        .withColumn("_value_num", lit(changeValue))
        .withColumn("_date_created", col("_date_created") + expr("INTERVAL 2 HOURS"))
        .drop(col("_hoodie_commit_seqno")).drop(col("_hoodie_commit_time"))
        .drop(col("_hoodie_record_key")).drop(col("_hoodie_partition_path"))
        .drop(col("_hoodie_file_name"))

      updateDs
        .select(to_json(struct("*")).as("value"))
        .selectExpr(s"CAST('${UUID.randomUUID().toString}' AS STRING)", "CAST(value AS STRING)")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", "b-1.dev-hudipoc-ms.vto656.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092,b-2.dev-hudipoc-ms.vto656.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092")
        .option("topic", "kafka_hudi_tag")
        .save()

      println(LocalDateTime.now() + s"===== finish write data into kafka, index $i")
    }

    spark.stop()
  }
}