package com.convertlab.hbase

import com.convertlab.common.{DataUtils, Tag}
import com.convertlab.gdm.constants.HbaseTableColumnFamilies
import com.convertlab.spark.runtime.utils.HbaseOperator
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import com.convertlab.spark.runtime.utils.HbaseOperator.HbaseRddBulkLoadFunc
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}

object HBaseTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder.enableHiveSupport()
      .appName("HBaseTest")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val traitId = "test_trait"
    val tagSchema = Tag.getTagSchema
    val tagRdd = sc.parallelize(0 until 6000, 100).map(i => DataUtils.fillData(tagSchema, i, traitId = traitId))
    val dataSet = spark.createDataFrame(tagRdd, tagSchema)
    val columnFamily = Bytes.toBytes("id")
    val qualifier = Bytes.toBytes(traitId)

    val hbaseConfiguration = HBaseConfiguration.create()
    hbaseConfiguration.set("hbase.zookeeper.quorum", "10.76.242.44,10.76.242.43,10.76.242.45")
    hbaseConfiguration.set("zookeeper.znode.parent",  "/hbase")
    val hbaseContext = new HBaseContext(sc, hbaseConfiguration)

    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    HbaseOperator.setHbaseNameSpace("hudi")
    HbaseOperator.setHbaseContext(hbaseContext)
    HbaseOperator.setFileSystem(fs)

    dataSet.insertHbaseSingleColumn("hudi_test", row => {
      val rowKey = Bytes.toBytes(s"${
        row.getString(0)
      }")
      val value = Bytes.toBytes((row.get(1)).toString)
      val put = new Put(rowKey)
      put.addColumn(columnFamily, qualifier, value)
    }, row => {
      val rowKey = Bytes.toBytes(s"${
        row.getString(0)
      }")
      val value = Bytes.toBytes((row.get(1)).toString)
      Seq((new KeyFamilyQualifier(rowKey, columnFamily, qualifier), value)).iterator
    })

    spark.stop()
  }

}