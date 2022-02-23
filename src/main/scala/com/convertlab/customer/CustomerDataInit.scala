package com.convertlab.customer

import java.time.LocalDateTime

import com.convertlab.common.{Customer, DataUtils, HudiTable}
import org.apache.spark.sql.SparkSession

object CustomerDataInit {

  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("CustomerDataInit")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    HudiTable.setProperties(Map(
      "tableName" -> "customer",
      "primaryKeys" -> "_id",
      "partitionKeys" -> "",
      "preCombineKey" -> "_date_created",
      "tableType" -> "MERGE_ON_READ",
    ))

    println(LocalDateTime.now() + " === start creating data for customer table: customer")
    // 写入客户数据。 1亿， 没有分区，没有主键
    val customerSchema = Customer.getCustomerSchema
    val customerRdd = sc.parallelize(0 until 100000000, 100).map(i => DataUtils.fillData(customerSchema, i))

    val customerDataFrame = spark.createDataFrame(customerRdd, customerSchema)

    println(customerDataFrame.schema.fieldNames.mkString(","))
//    println(customerDataFrame.first())

//    customerDataFrame.write.mode(SaveMode.Overwrite).parquet(Customer.tempFilePath)
//
//    println(LocalDateTime.now() + s"===== finish write parquet file")
//    val insertDs = spark.read.parquet(Customer.tempFilePath)
    println(LocalDateTime.now() + s"===== finish read parquet file")
    HudiTable.write(customerDataFrame, "bulk_insert")

    spark.stop()
  }

}
