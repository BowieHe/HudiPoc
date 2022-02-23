package com.convertlab.customer

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import java.time.LocalDateTime
import com.convertlab.common.{Customer, HudiTable}

object WriteStreamCustomerData {
  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("WriteStreamCustomerData")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schema = Customer.getCustomerSchema

    HudiTable.setProperties(Map(
      "tableName" -> "customer",
      "primaryKeys" -> "_id",
      "partitionKeys" -> "",
      "preCombineKey" -> "_date_created",
      "tableType" -> "MERGE_ON_READ",
    ))

    HudiTable.writeKafka(spark, schema, "kafka_hudi_customer")
  }
}