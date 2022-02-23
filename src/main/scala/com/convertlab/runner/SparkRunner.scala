package com.convertlab.runner

import com.convertlab.customer.CustomerDataInit
import org.apache.spark.sql.SparkSession

object SparkRunner {

//  private val CUSTOMER_DATA_INIT = "customer-data-init"
//
//  @throws[Exception]
//  def main(args: Array[String]): Unit = {
//    def index = System.getProperty("job_name").lastIndexOf("-")
//
//    def jobName = System.getProperty("job_name").substring(0, index)
//
//    val spark = SparkSession
//      .builder
//      .appName(jobName)
//      .getOrCreate()
//    val sc = spark.sparkContext
//
//    jobName match {
//      case CUSTOMER_DATA_INIT =>
//        new CustomerDataInit(spark, sc).execute()
//    }
//  }
}
