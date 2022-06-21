package org.example

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Sparkhive {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c://hadoop//")

    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", "SparkHiveTests").
      set("spark.driver.host", "localhost").
    set("spark.sql.warehouse.dir","C:/tmp/hive").
    set("spark.hadoop.hive.metastore.warehouse.dir","C:/hadoop/bin/metastore_db_2")

    //val sparkContext = new SparkContext(sparkConf)


    val spark = SparkSession.builder
      .config(sparkConf).enableHiveSupport().getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS employeedb")
    spark.sql("CREATE TABLE IF NOT EXISTS employeedb.employee(empid int,empage int,empname string")
  }

}
