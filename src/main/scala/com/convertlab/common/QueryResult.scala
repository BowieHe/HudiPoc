package com.convertlab.common

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object QueryResult {

  HudiTable.setProperties(Map(
    "tableName" -> "query_result",
    "primaryKeys" -> "t1,t2,t3,d1,d2,d3",
    "partitionKeys" -> "task_id",
    "preCombineKey" -> "_date_created",
    // "tableType" -> "COPY_ON_WRITE",
    "tableType" -> "COPY_ON_WRITE"
  ))

  def getQuerySchema: StructType = {
    val schema = StructType(Seq(
      StructField("t1", StringType),
      StructField("t2", StringType),
      StructField("t3", StringType),
      StructField("d1", StringType),
      StructField("d2", StringType),
      StructField("d3", StringType),
      StructField("_date_created", TimestampType),
      StructField("task_id", StringType)
    ))
    schema
  }
}