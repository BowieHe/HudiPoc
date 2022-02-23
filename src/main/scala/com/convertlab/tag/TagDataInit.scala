package com.convertlab.tag

import java.time.LocalDateTime
import java.util.UUID

import com.convertlab.common.{Customer, DataUtils, Event, HudiTable, Tag}
import org.apache.spark.sql.SparkSession

object TagDataInit {

  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("TagDataInit")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    HudiTable.setProperties(Map(
      "tableName" -> "tag",
      "primaryKeys" -> "_profile_id",
      "partitionKeys" -> "_trait_id",
      "preCombineKey" -> "_date_created",
      "tableType" -> "MERGE_ON_READ",
    ))

    println(LocalDateTime.now() + " === start creating data for customer table: customer")
    // 写入客户数据。 1亿， 没有分区，没有主键
    val tagSchema = Tag.getTagSchema
    val tagRdd = sc.parallelize(0 until 100000000, 100).map(i => DataUtils.fillData(tagSchema, i, traitId = UUID.randomUUID().toString.substring(0, 4)))

    val tagDataFrame = spark.createDataFrame(tagRdd, tagSchema)

    println(tagDataFrame.schema.fieldNames.mkString(","))
    tagDataFrame.createOrReplaceTempView("temp_tag")

    println(LocalDateTime.now() + s"===== finish read parquet file")
    HudiTable.write(tagDataFrame, "bulk_insert")

    spark.stop()
  }

}
