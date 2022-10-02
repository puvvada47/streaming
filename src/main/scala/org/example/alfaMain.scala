package org.example
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.eventhubs._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ListBuffer

object alfaMain {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().
      setMaster("local[*]").
      setAppName("test").
      set("spark.ui.enabled", "false").
      set("spark.app.id", "SparkHiveTests").
      set("spark.driver.host", "localhost")

    val sparkContext = new SparkContext(sparkConf)


    val spark = SparkSession.builder
      .config(sparkConf).getOrCreate()
    //Config notebook


    val mappingFilepath = "/dbfs/mnt/ddlasmldsmyle/dldata/etl/mapping/mapping_file.json"
    // Create EventHub Consumer

    try{

      // Extracting the table information by parsing the config file
      val configjsonstr: String = scala.io.Source.fromFile(mappingFilepath).mkString
      val configmap: List[Map[String, Any]] = parse(configjsonstr).values.asInstanceOf[Map[String, Any]]("tableInfo").asInstanceOf[List[Map[String, Any]]]

      var mutableListOfTableInfo: ListBuffer[(String, List[String], List[Map[String, Any]], List[String], List[String])] = new ListBuffer[(String, List[String], List[Map[String, Any]], List[String], List[String])]()

      configmap.foreach(x => {
        val tablename: String = x("tablename").asInstanceOf[String]
        val pseudocols: List[String] = x("pseudocols").asInstanceOf[List[String]]
        val scramblecols: List[Map[String, Any]] = x("scramblecols").asInstanceOf[List[Map[String, Any]]]
        val deletecols: List[String] = x("deletecols").asInstanceOf[List[String]]
        val key: List[String] = x("key").asInstanceOf[List[String]]

        mutableListOfTableInfo += ((tablename, pseudocols, scramblecols, deletecols, key))
      })

      // List having table information
      val listOfTableInfo: List[(String, List[String], List[Map[String, Any]], List[String], List[String])] = mutableListOfTableInfo.toList

      // Creating an EventHub connection string
      val endpoint = "Endpoint=sb://dlzasmldsmyle.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xv7f4hmY/wg6DUEAGaDGdIfEKs7YCFcw2bXM0m/6Xrw="

      val connectionString = ConnectionStringBuilder(endpoint)
        .setEventHubName("dlzasmldsmyle-hub")
        .build

      val eventhubConf = EventHubsConf(connectionString).setConsumerGroup("$Default").setStartingPosition(EventPosition.fromStartOfStream).setMaxEventsPerTrigger(10000)

      // Need to set the consumer to read from the latest offset
      // val eventhubConf = EventHubsConf(connectionString).setConsumerGroup("$Default")

      // Creating a DStream of dataframes
      val df = spark
        .readStream
        .format("eventhubs")
        .options(eventhubConf.toMap)
        .load()

      // df.printSchema

      val messages = df.withColumn("Offset", col("offset").cast(LongType)) .withColumn("Time", col("enqueuedTime").cast(TimestampType)) .withColumn("Timestamp", col("enqueuedTime").cast(LongType)) .withColumn("Body", col("body").cast(StringType)) .select("Offset", "Time", "Timestamp", "Body")


      messages.writeStream.foreachBatch ((batchDF: DataFrame, batchId: Long) =>{ AlfaStream.processDf(batchDF, listOfTableInfo)}).start()

    } catch {
      case e: Exception => println("DATA missing, " + e)
    }
  }

}


