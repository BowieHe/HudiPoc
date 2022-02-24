package com.convertlab.tag

import java.time.LocalDateTime
import java.util.UUID

import com.convertlab.common.{HudiTable, QueryResult}
import org.apache.spark.sql.SparkSession

object TagJoinCustomerQuery {
  def main(args: Array[String]): Unit = {

    QueryResult
    println("==== start processing")
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("TagJoinCustomerQuery")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val uuid = UUID.randomUUID().toString.substring(0, 4)
    val sql = s"SELECT  _value_text     AS t1, " +
      s"cast(`_value_num` as string)      AS d1, " +
      s"_trait_id        AS t2, " +
      s"cast(COUNT(1) as string)        AS d2, " +
      s"cast(SUM(_value_num) as string) AS `d3`, " +
      s"''  AS t3, " +
      s"current_timestamp AS `_date_created`, " +
      s"'$uuid'  AS `task_id` " +
      s"FROM tag " +
      s"WHERE _trait_id IN ('9816') " +
      s"AND _profile_id IN ( SELECT  _id " +
      s"    FROM customer_ro temp_main left semi" +
      s"    JOIN" +
      s"    (" +
      s"        SELECT  temp._id" +
      s"        FROM" +
      s"        (" +
      s"            SELECT  _id" +
      s"           FROM customer_ro t1 left semi" +
      s"            JOIN" +
      s"            (" +
      s"                SELECT  _profile_id AS _id" +
      s"                FROM tag" +
      s"                WHERE _trait_id = '9816' " +
      s"            ) t2 " +
      s"           ON t1._id=t2._id" +
      s"        ) temp left semi" +
      s"        JOIN" +
      s"        (" +
      s"            SELECT  _id" +
      s"            FROM customer_ro" +
      s"            WHERE (_id is not null AND _id !='') " +
      s"        ) temp1" +
      s"        ON temp._id = temp1._id" +
      s"    ) temp0" +
      s"    ON temp_main._id=temp0._id) " +
      s"GROUP BY  _value_text" +
      s"         ,_value_num" +
      s"         ,_trait_id"

    val dataset = spark.sql(sql)

    println(HudiTable.tableName)
    println(LocalDateTime.now() + s"===== end execute sql")
    println(LocalDateTime.now() + s"${dataset.first()}")

    HudiTable.write(dataset, "bulk_insert")

  }
}