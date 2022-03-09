package com.convertlab.common

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Customer {

  val tempFilePath = "s3://dev-hudipoc-emr-logs/tmp/hudi/poc/temp/temp_customer.parquet"
  val tablePath = "s3://dev-hudipoc-emr-logs/tmp/hudi/poc/customer"

  HudiTable.setProperties(Map(
    "tableName" -> "customer",
    "primaryKeys" -> "_id",
    "partitionKeys" -> "",
    "preCombineKey" -> "_date_created",
    "tableType" -> "MERGE_ON_READ"
  ))

  def getCustomerSchema: StructType = {
    var schema = StructType(Seq(
      StructField("_id", StringType),
      StructField("_last_updated_batch_id", StringType),
      StructField("_email", StringType),
      StructField("_name", StringType),
      StructField("_img", StringType),
      StructField("_last_updater_id", StringType),
      StructField("_import_method", StringType),
      StructField("_create_from", StringType),
      StructField("_creator_id", StringType),
      StructField("_mobile", StringType),
      StructField("_gender", StringType),
      StructField("_date_created", TimestampType),
      StructField("_last_updated", TimestampType),
      StructField("_birthday", TimestampType)
    ))
    for(i <- 1 to 30) {
      //columns ++= Array[String](s"attr$i", s"str$i", s"num$i")
      schema = schema.add(StructField(s"attr$i", StringType)).add(StructField(s"str$i", StringType)).add(StructField(s"num$i", IntegerType))
    }
    schema
  }
}