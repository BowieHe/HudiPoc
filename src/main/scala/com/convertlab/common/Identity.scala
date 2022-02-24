package com.convertlab.common

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

object Identity {

  HudiTable.setProperties(Map(
    "tableName" -> "identity",
    "primaryKeys" -> "_id,_type",
    "partitionKeys" -> "",
    "preCombineKey" -> "_date_created",
    "tableType" -> "MERGE_ON_READ",
  ))

  def getIdentitySchema: StructType = {
    val schema = StructType(Seq(
      StructField("_id", StringType),
      StructField("_type", StringType),
      StructField("_value", StringType),
      StructField("_date_created", TimestampType)
    ))
    schema
  }

}