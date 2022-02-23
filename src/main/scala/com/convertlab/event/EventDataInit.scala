package com.convertlab.event

import java.time.LocalDateTime

import com.convertlab.common.{Customer, DataUtils, Event, HudiTable}
import org.apache.spark.sql.SparkSession

object EventDataInit {

  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("EventDataInit")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    HudiTable.setProperties(Map(
      "tableName" -> "event",
      "primaryKeys" -> "_external_id,_event_type",
      "partitionKeys" -> "partition",
      "preCombineKey" -> "_date_created",
      "tableType" -> "MERGE_ON_READ",
    ))

    println(LocalDateTime.now() + " === start creating data for customer table: customer")
    // 写入客户数据。 1亿， 没有分区，没有主键
    val eventSchema = Event.getEventSchema
    val eventRdd = sc.parallelize(0 until 300000000, 100).map(i => DataUtils.fillData(eventSchema, i))

    val eventDataFrame = spark.createDataFrame(eventRdd, eventSchema)

    println(eventDataFrame.schema.fieldNames.mkString(","))
    eventDataFrame.createOrReplaceTempView("temp_customer")

    println(LocalDateTime.now() + s"===== finish read parquet file")
    HudiTable.write(eventDataFrame, "bulk_insert")

    spark.stop()
  }

}
