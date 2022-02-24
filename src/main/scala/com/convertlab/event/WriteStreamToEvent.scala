package com.convertlab.event

import com.convertlab.common.{Event, HudiTable, Tag}
import org.apache.spark.sql.SparkSession

object WriteStreamToEvent {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("WriteStreamToEvent")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schema = Event.getEventSchema

    HudiTable.writeKafka(spark, schema, "kafka_hudi_event")
  }
}