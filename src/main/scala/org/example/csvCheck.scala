package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object csvCheck {
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



    //val schema: StructType = StructType(schemaString.split(" ").map(fieldName =>  StructField(fieldName, StringType, true)))

    val schemaString = "name dep age org1"
    val fixedSchema=schemaString.split(" ")
    val colFixedSchema: Array[Column] =fixedSchema.map(field=>(col(field)*0.013).as("USD1"))
    val df=spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load("C:/Users/KPUVVAD/Desktop/emp.csv")


    //val empEncoder: Encoder[Emp] = product[Emp]

   //val ds: Dataset[Emp] =spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load("C:/Users/KPUVVAD/Desktop/emp.csv").as(empEncoder)



    val dfSchema: Array[String] =df.columns.map(e=>e.toLowerCase)
    val fixDiffSchema: Array[String] =fixedSchema.diff(dfSchema)

    val dfDiffSchema=dfSchema.diff(fixedSchema)
    val dropdf=df.drop(dfDiffSchema:_*)
    val finaldf: DataFrame =fixDiffSchema.foldLeft(dropdf)((dropdf, e)=>dropdf.withColumn(e,lit(null)))

    //val finalDF=dfDiffSchema.foldLeft(newDf)((newDf,e)=>newDf.drop(e))
    finaldf.select(colFixedSchema:_*).show()







    //---------------------------------------------
    //adding new columns along with old columns
    //---------------------------------------------

    /*val xfactor=0.013
    val oldCols: List[String] =List("lc1","lc2","lc3")
    val newCols: List[String] =List("USD1","USD2","USD3")
    val newOldcolumns: List[Column] =oldCols.zip(newCols).flatMap(field=>List(col(field._1),(col(field._1)*0.013).as(field._2)))
    finaldf.select(newOldcolumns:_*)*/








  }

}


