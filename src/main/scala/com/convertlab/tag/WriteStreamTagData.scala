package com.convertlab.tag

import com.convertlab.common.{HudiTable, Tag}
import org.apache.spark.sql.SparkSession

object WriteStreamTagData {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("WriteStreamTagData")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schema = Tag.getTagSchema

    HudiTable.writeKafka(spark, schema, "kafka_hudi_tag")
  }
}