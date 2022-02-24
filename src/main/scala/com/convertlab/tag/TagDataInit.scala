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

    val traitId = UUID.randomUUID().toString.substring(0, 4)

    println(LocalDateTime.now() + s" === start creating data for customer table: trait, and traitId $traitId")
    // 写入客户数据。 1亿， 没有分区，没有主键
    val tagSchema = Tag.getTagSchema

    val tagRdd = sc.parallelize(0 until 100000000, 100).map(i => DataUtils.fillData(tagSchema, i, traitId = traitId))

    val tagDataFrame = spark.createDataFrame(tagRdd, tagSchema)

    println(tagDataFrame.schema.fieldNames.mkString(","))
    tagDataFrame.createOrReplaceTempView("temp_tag")


    HudiTable.write(tagDataFrame, "bulk_inserprintln(LocalDateTime.now() + s\"===== finish read parquet file\")t")

    spark.stop()
  }

}
