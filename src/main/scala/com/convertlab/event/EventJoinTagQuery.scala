package com.convertlab.event

import java.time.LocalDateTime
import java.util.UUID

import com.convertlab.common.{HudiTable, QueryResult}
import org.apache.spark.sql.SparkSession

object EventJoinTagQuery {
  def main(args: Array[String]): Unit = {

    QueryResult
    println("==== start processing")
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("EventJoinTagQuery")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val uuid = UUID.randomUUID().toString.substring(0, 4)
    println(s"====insert task id: ${uuid}")
    val sql = "SELECT  cast(count(event_ro_order_date) as string) as d1" +
      "       ,event_ro_event_type as d2" +
      "       ,cast(sum(event_ro_external_id) as string) as d3" +
      "       ,cast(date_format(from_utc_timestamp(event_ro_date_created,'Asia/Shanghai'),'yyyy-MM-dd') as string) AS t1" +
      "       ,cast(COUNT(distinct customer_ro_car_name) as string)     AS t2," +
      s"      ''  AS t3, " +
      s"      current_timestamp AS `_date_created`, " +
      s"'$uuid'  AS `task_id` " +
      "FROM " +
      "(" +
      " SELECT  event_ro.`_order_date` AS `event_ro_order_date`" +
      "           ,event_ro.`_external_id` AS `event_ro_external_id`" +
      "       ,customer_ro.`_name`  AS `customer_ro_car_name`" +
      "       ,customer_ro.`_import_method`  AS `customer_ro_import_method`" +
      "       ,customer_ro.`_creator_id`  AS `customer_ro_creator_id`" +
      "      ,event_ro.`_date_created`    AS `event_ro_date_created`" +
      "         ,event_ro.`_event_type`     AS `event_ro_event_type` " +
      "FROM " +
      "( " +
      "SELECT  * " +
      "FROM event_ro " +
      "WHERE " +
      " )event_ro" +
      " LEFT JOIN customer_ro " +
      "ON event_ro._profile_id=customer_ro._id" +
      ")g1" +
//      " WHERE event_ro_order_date>='2021-02-02 00:00:00' " +
//      "AND event_ro_order_date<='2022-02-24 07:03:04' " +
      " GROUP BY  `customer_ro_import_method`" +
      "         ,`event_ro_event_type`" +
      "         ,date_format(from_utc_timestamp(event_ro_date_created,'Asia/Shanghai'),'yyyy-MM-dd');"

    val dataset = spark.sql(sql)

    println(HudiTable.tableName)
    println(LocalDateTime.now() + s"===== end execute sql")
    println(LocalDateTime.now() + s"${dataset.first()}")

    HudiTable.write(dataset, "bulk_insert")
  }
}