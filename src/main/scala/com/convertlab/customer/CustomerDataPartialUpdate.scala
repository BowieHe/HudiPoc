package com.convertlab.customer

import java.time.LocalDateTime
import java.util.UUID

import com.convertlab.common.HudiTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustomerDataPartialUpdate {

  def main(args: Array[String]): Unit = {
    println("==== start processing")
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("CustomerDataPartialUpdate")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // select 50,000,000 customer data from customer
    spark.sql("use default")
    spark.sql("show tables")
    val originDs = spark.sql("select * from customer_ro")
      .orderBy(rand()).limit(30000000)
      .withColumn("_img", lit(UUID.randomUUID().toString))
      .withColumn("_date_created", col("_date_created") + expr("INTERVAL 2 HOURS"))
      .drop(col("_hoodie_commit_seqno")).drop(col("_hoodie_commit_time"))
      .drop(col("_hoodie_record_key")).drop(col("_hoodie_partition_path"))
      .drop(col("_hoodie_file_name"))

    //    originDs.write.mode(SaveMode.Overwrite).parquet(Customer.tempFilePath)
    //
    //    println(LocalDateTime.now() + s"===== finish write parquet file")
    //    val insertDs = spark.read.parquet(Customer.tempFilePath)
    println(LocalDateTime.now() + s"===== finish read parquet file")

    HudiTable.write(originDs, "upsert")

    spark.stop()
  }

}
