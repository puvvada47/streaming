package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object csvCheckRDD {
  case class Emp(name: String)
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", "SparkHiveTests").
      set("spark.driver.host", "localhost")


    val sc = new SparkContext(sparkConf)
     val rdd: RDD[String] =  sc.textFile("C:/Users/KPUVVAD/Desktop/emp.csv")
    println(s"no of partitions: ${rdd.getNumPartitions}")
    val rdd1=rdd.coalesce(10)
    println(s"no of partitions after coalesce: ${rdd1.getNumPartitions}")








   /*
    if partitions on RDD are 2
    then if we do coalesce(4) leading to nothing happens and still no of partitions will be 2
    */







  }

}


