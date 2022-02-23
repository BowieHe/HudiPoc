package com.convertlab.tag

import com.convertlab.common.{HudiTable, Tag}
import org.apache.spark.sql.SparkSession

object WriteStreamTagData {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("WriteStreamCustomerData")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schema = Tag.getTagSchema

    HudiTable.setProperties(Map(
      "tableName" -> "tag",
      "primaryKeys" -> "_profile_id",
      "partitionKeys" -> "_trait_id",
      "preCombineKey" -> "_date_created",
      "tableType" -> "MERGE_ON_READ",
    ))

    HudiTable.writeKafka(spark, schema, "kafka_hudi_tag")
  }
}