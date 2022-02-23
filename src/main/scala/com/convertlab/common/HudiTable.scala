package com.convertlab.common

import java.time.LocalDateTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable

object HudiTable {

  var tableName: String = ""
  var primaryKeys: String = ""
  var partitionKeys = ""
  var preCombineKey = "_date_created"
  var tableType: String = "MERGE_ON_READ"

  def setProperties(properties: Map[String, AnyRef]): Unit = {
    val keys = properties.keySet
    keys.foreach {
      case "tableName" =>
        this.tableName = properties("tableName").toString
      case "primaryKeys" =>
        this.primaryKeys = properties("primaryKeys").toString
      case "partitionKeys" =>
        this.partitionKeys = properties("partitionKeys").toString
      case "preCombineKey" =>
        this.preCombineKey = properties("preCombineKey").toString
      case "tableType" =>
        this.tableType = properties("tableType").toString
    }
  }

  def write(dataset: Dataset[Row], writeMode: String, lookRequired: Boolean = false): Unit = {

    var options = mutable.Map(
      "hoodie.datasource.write.table.type" -> tableType,
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.operation" -> writeMode,
      "hoodie.datasource.write.recordkey.field" -> primaryKeys,
      "hoodie.datasource.write.precombine.field" -> preCombineKey,
      "hoodie.clean.async" -> "true",
      "hoodie.clean.automatic" -> "true",
      "hoodie.cleaner.commits.retained" -> "2",
      "hoodie.keep.min.commits" -> "3",
      "hoodie.keep.max.commits" -> "4",
      "hoodie.datasource.hive_sync.enable" -> "true",
      "hoodie.datasource.hive_sync.mode" -> "hms",
      // TODO change the dataset if necessary
      "hoodie.datasource.hive_sync.database" -> "default",
      "hoodie.datasource.hive_sync.support_timestamp" -> "true",
      "hoodie.datasource.hive_sync.table" -> tableName,
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.DefaultHoodieRecordPayload"
    )

    if (lookRequired) {
      options ++= mutable.Map(
        "hoodie.write.concurrency.mode" -> "optimistic_concurrency_control",
        "hoodie.cleaner.policy.failed.writes" -> "LAZY",
        "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider",
        "hoodie.write.lock.zookeeper.url" -> "z-1.dev-hudipoc-ms.vto656.c4.kafka.cn-northwest-1.amazonaws.com.cn",
        "hoodie.write.lock.zookeeper.port" -> "2181",
        "hoodie.write.lock.zookeeper.lock_key" -> tableName,
        "hoodie.write.lock.zookeeper.base_path" -> "/hudi/write_lock")
    }

    if (partitionKeys != "") {
      options ++= mutable.Map(
        "hoodie.datasource.write.partitionpath.field" -> partitionKeys,
        "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.ComplexKeyGenerator",
        "hoodie.datasource.write.hive_style_partitioning" -> "true",
        "hoodie.datasource.hive_sync.partition_fields" -> partitionKeys,
        "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.MultiPartKeysValueExtractor"
      )
    } else {
      options ++= mutable.Map(
        "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
        "hoodie.datasource.hive_sync.partition_extractor_class" -> "org.apache.hudi.hive.NonPartitionedExtractor"
      )
    }

    dataset.persist(StorageLevel.MEMORY_AND_DISK_SER)
    // println(s"dataset first row ${dataset.first().mkString(",")} and get count ${dataset.count()}")
    println(LocalDateTime.now() + "===== start write hudi table")
    dataset.write.format("hudi").options(options)
      .mode(SaveMode.Append)
      .save(s"s3://dev-hudipoc-emr-logs/tmp/hudi/poc/$tableName")
    println(LocalDateTime.now() + "===== finish write hudi table")
  }

  def writeKafka(spark: SparkSession, schema:StructType, kafkaTopic: String): Unit = {
    val dataStreamReader = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "b-1.dev-hudipoc-ms.vto656.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092,b-2.dev-hudipoc-ms.vto656.c4.kafka.cn-northwest-1.amazonaws.com.cn:9092")
      .option("subscribe", kafkaTopic)
      .option("maxOffsetsPerTrigger", 100000)
      .load()

    val kafkaData = dataStreamReader.withColumn("json", col("value").cast(StringType))
      .select(from_json(col("json"), schema) as "data")
      .select("data.*").where("_date_created is not null")

    val query = kafkaData
      .writeStream
      .queryName(kafkaTopic)
      .foreachBatch { (batchDF: DataFrame, _: Long) => {

        batchDF.persist()

        println(LocalDateTime.now() + " === start writing table")
        HudiTable.write(batchDF, "upsert", lookRequired = true)
        batchDF.unpersist()
        println(LocalDateTime.now() + " === finish")
      }
      }
      .option("checkpointLocation", "/tmp/hudi/checkpoint/")
      .start()

    query.awaitTermination()
  }

}