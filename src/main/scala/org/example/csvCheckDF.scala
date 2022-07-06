package org.example

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.immutable

object csvCheckDF {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", "SparkHiveTests").
      set("spark.driver.host", "localhost")


    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()


    val schemaString: String = "name dep age org1"
    val fixedSchema: Array[String] = schemaString.split(" ")
    val listFixedSchema: List[Column] = fixedSchema.toList.map(field => col(field))
    //val schema: StructType = StructType(schemaString.split(" ").map(fieldName =>  StructField(fieldName, StringType, true)))
    val df = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load("C:/Users/KPUVVAD/Desktop/emp.csv")


    //val empEncoder: Encoder[Emp] = product[Emp]

    //val ds: Dataset[Emp] =spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").load("C:/Users/KPUVVAD/Desktop/emp.csv").as(empEncoder)


    val dfSchema: Array[String] = df.columns.map(e => e.toLowerCase)
    val fixDiffSchema: Array[String] = fixedSchema.diff(dfSchema)

    val dfDiffSchema = dfSchema.diff(fixedSchema)
    val dropDF = df.drop(dfDiffSchema: _*)
    //val finalDF=dfDiffSchema.foldLeft(df)((df,field)=>newDf.drop(field))
    val finalDF: DataFrame = fixDiffSchema.foldLeft(dropDF)((dropDF, e) => dropDF.withColumn(e, lit(null)))
    val otherDF = finalDF.select(listFixedSchema:_*)
    otherDF.show()
    val sampleDF = finalDF
    sampleDF.show(false)








    //---------------------------------------------
    //adding new newColumns along with old newColumns
    //---------------------------------------------

    /*val xfactor=0.013
    val oldCols: List[String] =List("lc1","lc2","lc3")
    val newCols: List[String] =List("USD1","USD2","USD3")
    val newOldcolumns: List[Column] =oldCols.zip(newCols).flatMap(field=>List(col(field._1),(col(field._1)*0.013).as(field._2)))
    finalDF.select(newOldcolumns:_*)*/


  }

  case class Emp(name: String)

}


