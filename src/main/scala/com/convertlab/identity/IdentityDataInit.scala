package com.convertlab.identity

import java.time.LocalDateTime

import com.convertlab.common.{DataUtils, HudiTable, Identity}
import org.apache.spark.sql.SparkSession

object IdentityDataInit {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("IdentityDataInit")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    println(LocalDateTime.now() + " === start creating data for customer table: identity")
    // 写入客户数据。 1亿， 没有分区，没有主键
    val identitySchema = Identity.getIdentitySchema
    val identityRdd = sc.parallelize(0 until 1000000000, 100).map(i => DataUtils.fillData(identitySchema, i))

    val identityDataFrame = spark.createDataFrame(identityRdd, identitySchema)

    println(identityDataFrame.schema.fieldNames.mkString(","))


    println(LocalDateTime.now() + s"===== finish read parquet file")
    HudiTable.write(identityDataFrame, "bulk_insert")

    spark.stop()
  }
}