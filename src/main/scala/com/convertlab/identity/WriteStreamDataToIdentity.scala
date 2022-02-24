package com.convertlab.identity

import org.apache.spark.sql.SparkSession
import com.convertlab.common.{Identity, HudiTable}

object WriteStreamDataToIdentity {
  def main(args: Array[String]): Unit = {
    Identity
    println("==== start processing")
    val spark = SparkSession
      .builder
      .appName("WriteStreamDataToIdentity")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val schema = Identity.getIdentitySchema

    HudiTable.writeKafka(spark, schema, "kafka_hudi_identity")
  }
}