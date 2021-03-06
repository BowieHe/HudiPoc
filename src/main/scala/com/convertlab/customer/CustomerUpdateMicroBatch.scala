package com.convertlab.customer

import java.time.LocalDateTime
import com.convertlab.common.{Customer, DataUtils, HudiTable}
import org.apache.spark.sql.SparkSession

object CustomerUpdateMicroBatch {

  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("CustomerUpdateMicroBatch")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val customerSchema = Customer.getCustomerSchema
    val customerRdd = sc.parallelize(0 until 3000, 100).map(i => DataUtils.fillData(customerSchema, (i * 30000)))

    val customerDataFrame = spark.createDataFrame(customerRdd, customerSchema)
    println(customerDataFrame.schema)
    println(customerDataFrame.schema.fieldNames.mkString(","))
    customerDataFrame.createOrReplaceTempView("temp_customer")

    println(s"get customer data frame count ${customerDataFrame.count()}")
    println(LocalDateTime.now() + s"===== finish read parquet file")
    HudiTable.write(customerDataFrame, "upsert", lookRequired = true)
    spark.stop()
  }

}
