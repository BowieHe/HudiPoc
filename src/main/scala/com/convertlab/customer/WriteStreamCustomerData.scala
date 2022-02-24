package com.convertlab.customer

import org.apache.spark.sql.SparkSession
import com.convertlab.common.{Customer, HudiTable}

object WriteStreamCustomerData {
  def main(args: Array[String]): Unit = {
    Customer
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("WriteStreamCustomerData")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schema = Customer.getCustomerSchema

    HudiTable.writeKafka(spark, schema, "kafka_hudi_customer")
  }
}