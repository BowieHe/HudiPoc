package com.convertlab.event

import java.time.LocalDateTime
import java.util.UUID

import com.convertlab.common.{DataUtils, Event, HudiTable}
import org.apache.spark.sql.SparkSession

object EventConcurrentUpdate {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("EventConcurrentUpdate")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val eventType = UUID.randomUUID().toString.substring(0 ,4)
    println(LocalDateTime.now() + " === start creating data for customer table: event")
    println(LocalDateTime.now() + s" === create new event type $eventType")
    // 写入客户数据。 1亿， 没有分区，没有主键
    val eventSchema = Event.getEventSchema
    println(eventSchema)
    val eventRdd = sc.parallelize(0 until 10000000, 100).map(i => DataUtils.fillData(eventSchema, i, eventType = eventType))

    val eventDataFrame = spark.createDataFrame(eventRdd, eventSchema)
    println(eventDataFrame.schema)
    println(eventDataFrame.schema.fieldNames.mkString(","))
    eventDataFrame.createOrReplaceTempView("temp_event")

    println(LocalDateTime.now() + s"===== finish read parquet file")
    HudiTable.write(eventDataFrame, "upsert", lookRequired = true)

    spark.stop()
  }
}