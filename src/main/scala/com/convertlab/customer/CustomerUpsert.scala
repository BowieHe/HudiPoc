package com.convertlab.customer

import java.time.LocalDateTime
import com.convertlab.common.{Customer, DataUtils, HudiTable}
import org.apache.spark.sql.SparkSession

object CustomerUpsert {

  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("CustomerUpsert")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val customerSchema = Customer.getCustomerSchema
    val customerRdd = sc.parallelize(130000000 until 130003000, 100).map(i => DataUtils.fillData(customerSchema, i))

    val customerDataFrame = spark.createDataFrame(customerRdd, customerSchema)
    println(customerDataFrame.schema)
    println(customerDataFrame.schema.fieldNames.mkString(","))
    customerDataFrame.createOrReplaceTempView("temp_customer")

    println(LocalDateTime.now() + s"===== finish read parquet file")
    HudiTable.write(customerDataFrame, "upsert", lookRequired = true)
    spark.stop()
  }

}
