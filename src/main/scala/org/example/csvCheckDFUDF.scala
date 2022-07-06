package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

object csvCheckDFUDF {

  case class Emp(name: String)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", "SparkHiveTests").
      set("spark.driver.host", "localhost")


    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()


    val schemaString = "name dep age org1"
    //val schema: StructType = StructType(schemaString.split(" ").map(fieldName =>  StructField(fieldName, StringType, true)))

    val fixedSchema = schemaString.split(" ")
    val colFixedSchema: Array[Column] = fixedSchema.map(field => (col(field) * 0.013).as("USD1"))
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load("C:/Users/KPUVVAD/Desktop/emp.csv")

    //User Defined Function
    val convertCase: String => String = (str: String) => str.toLowerCase

    //defining UDF
    val convertUDF: UserDefinedFunction = udf(convertCase)

    //calling UDF
    df.select(convertUDF(col("name")).as("name")).show()


    //Registering Spark UDF to use it on SQL

    // Using it on SQL
    spark.udf.register("convertUDF", convertCase)
    df.createOrReplaceTempView("sampleTable")
    spark.sql("select  convertUDF(name) from sampleTable").show(false)


  }

}


