package com.convertlab.common

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Tag {

  val tablePath = "s3://dev-hudipoc-emr-logs/tmp/hudi/poc/tag"
  //    val tableName = "tag"
  val tableName = "tag_mor"

  HudiTable.setProperties(Map(
    "tableName" -> tableName,
    "primaryKeys" -> "_profile_id",
    "partitionKeys" -> "_trait_id",
    "preCombineKey" -> "_date_created",
    // "tableType" -> "COPY_ON_WRITE",
    "tableType" -> "MERGE_ON_READ"
  ))

  def getTagSchema: StructType = {
    val schema = StructType(Seq(
      StructField("_profile_id", StringType),
      StructField("_trait_id", StringType),
      StructField("_value_text", StringType),
      StructField("_value_num", IntegerType),
      StructField("_value_date", TimestampType),
      StructField("_date_created", TimestampType),
      StructField("_last_updated", TimestampType)
    ))
    schema
  }
}