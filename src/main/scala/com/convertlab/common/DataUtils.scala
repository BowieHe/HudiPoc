
package com.convertlab.common

import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import java.util.{Date, UUID}
import scala.collection.mutable.ListBuffer
import scala.util.Random

object DataUtils {

  val methodList = List("wechat", "alipay", "youZan", "JD")
  val eventTypeList = List("purchase", "surfing", "addCart", "refund", "makeOrder")
  val traitStrValue = List("adidas", "nike", "vans", "converts", "muji")

  def fillData(schema: StructType, i: Int, traitId: String = ""): Row = {
    val data = ListBuffer[Any]()
    val uuid = UUID.randomUUID().toString
    val name = uuid.substring(0, 6)
    val date = calFieldDate()
    val method = methodList(Random.nextInt(methodList.length))
    // val eventType = eventTypeList(Random.nextInt(eventTypeList.length))
    val attrReg = raw"attr(\d+)".r
    val numReg = raw"num(\d+)".r
    val strReg = raw"str(\d+)".r
    schema.fieldNames.foreach {
      case "_id" =>
        data += i.toString
      case "_external_id" =>
        data += i.toInt
      case "_profile_id" =>
        data += (if(i > 100000000) i / 3 else i).toString
      case "_last_updated_batch_id" | "_img" | "_last_updater_id" =>
        data += uuid
      case "_date_created" | "_last_updated" | "_birthday" | "_order_date" =>
        data += new Timestamp(date)
      case "_import_method" | "_create_from" =>
        data += method
      case "_event_type" =>
        data += eventTypeList(Random.nextInt(eventTypeList.length))
      case "_gender" =>
        data += Random.nextInt(1).toString
      case "_name" | "_create_from"| "_creator_id" =>
        data += name
      case "_email" =>
        data += s"$name@email.com"
      case "partition" =>
        data += new SimpleDateFormat("yyyy-MM").format(new Date(date))
      case attrReg(_*) =>
        data += s"attr$i"
      case numReg(_*) =>
        data += i.toInt
      case strReg(_*) =>
        data += s"str$i"
      // for trait
      case "_trait_id" =>
        data += traitId
      case "_value_num" =>
        data += Random.nextInt(10).toString
      case "_value_text" =>
        data += traitStrValue(Random.nextInt(traitStrValue.length))
      case "_value_date" =>
        data += new Timestamp(date)
      case _ =>
        data += s"unknown-$i"
    }
    Row.fromSeq(data)
  }

  def calFieldDate(): Long = {
    1609459200000L + Random.nextInt(63072000) * 1000L  // for diff month， from2021-01-01 ~ 2023-01-01
  }
}
