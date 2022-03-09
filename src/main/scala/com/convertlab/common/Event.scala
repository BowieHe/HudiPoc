package com.convertlab.common

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object Event {

  val tablePath = "s3://dev-hudipoc-emr-logs/tmp/hudi/poc/event"

  HudiTable.setProperties(Map(
    "tableName" -> "event",
    "primaryKeys" -> "_external_id,_event_type",
    "partitionKeys" -> "partition",
    "preCombineKey" -> "_date_created",
    "tableType" -> "MERGE_ON_READ"
  ))

  def getEventSchema: StructType = {
    var schema = StructType(Seq(
      StructField("_external_id", IntegerType),
      StructField("_profile_id", StringType),
      StructField("_last_updated_batch_id", StringType),
      StructField("_email", StringType),
      StructField("_name", StringType),
      StructField("_last_updater_id", StringType),
      StructField("_import_method", StringType),
      StructField("_create_from", StringType),
      StructField("_creator_id", StringType),
      StructField("_mobile", StringType),
      StructField("_gender", StringType),
      StructField("_date_created", TimestampType),
      StructField("_last_updated", TimestampType),
      StructField("_birthday", TimestampType),
      StructField("_event_type", StringType),
      StructField("_order_date", TimestampType),
      StructField("partition", StringType)
    ))
    for (i <- 1 to 20) {
      schema = schema.add(StructField(s"attr$i", StringType))
    }
    schema
  }
}