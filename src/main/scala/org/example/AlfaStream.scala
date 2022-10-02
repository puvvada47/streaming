package org.example


import io.delta.tables.DeltaTable
import org.apache.spark.rdd._
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._

import scala.collection.mutable.ListBuffer

//sample data in JSON
/*{
"before":null,
"after":{
"id":13039.0,
"thirdpartynumber":"5000000549",
"name":"Micky Marlin Inaon10736",
"exposuregrpcode":"<None>",
"exposuregrpname":"<None>",
"industrycode":"<None>",
"industryname":"<None>",
"companyregno":"<None>",
"collectionscode":"-",
"thirdpartytype":"IND",
"thirdpartytypename":"Individual",
"caislegalentitytype":"<None>",
"capallowcustomertype":"PLC",
"nationalidnumber":"<None>",
"addressline1":"Labergasse 125",
"addressline2":null,
"addressline3":null,
"addressline4":"Emsland",
"addressline5":null,
"addresstype":"Unknown",
"postalcode":"44444",
"countrycode":"DE",
"country":"Deutschland",
"postalregionname":"<Not applicable>",
"deliveryinfo":"<None>",
"etllogid":743617.0
},
"source":{
"version":"1.7.2.Final",
"connector":"postgresql",
"name":"psql-mbld-alfa-westeurope",
"ts_ms":1639736916758,
"snapshot":"false",
"db":"psql-ods-sandbox",
"sequence":"[\"677734779536\",\"677734779920\"]",
"schema":"public",
"table":"odsthirdparty",
"txId":143212643,
"lsn":677734779920,
"xmin":null
},
"op":"c",
"ts_ms":1643027644498,
"transaction":null
}

in JSON key->Jsvalue

*/

object AlfaStream {
  val processedFileRootPath = "/mnt/ddlasmldsmyle/dldata/etl_stream/processed/"
  // List of tables  to be extracted from source system
  val listOfSourceTables = List("odsassetclassification", "odsbillingaddress",  "odsschedulemain", "odsthirdparty",   "odsschedulerentalsummary", "odsschedulecashflow", "odscashinvoicewriteoff", "odsvehicle",  "odsterminationquote", "odspricingterms", "odsreschedulesummary", "odsreceivable", "odspayable",  "odsposting","odsscheduleendoftermsummary","odsschedulefloatratesummary", "odsschedulefundingsummary","odstmpmanufacturer","odstmpasset","odsvalueaddedproduct","odscurrency","odstmpagreement","odsscheduleratesummary","odstmpproduct","odstmpcurrentmiscinfos","odstmpcurrentmiscinfoasset","odstmpcurmiscinfobilladd")



  val sparkConf = new SparkConf().
    setMaster("local[*]").
    setAppName("test").
    set("spark.ui.enabled", "false").
    set("spark.app.id", "SparkHiveTests").
    set("spark.driver.host", "localhost")

  val sparkContext = new SparkContext(sparkConf)


  val spark = SparkSession.builder
    .config(sparkConf).getOrCreate()


  def getEnrichedTuple(beforeOrAfter: String, rawDataAsMap:Map[String,JsValue], tableName: String, operationType: String ) = {

    val msgContent: String = rawDataAsMap.get(beforeOrAfter).get.toString
    val messagetimestamp: String = rawDataAsMap.get("source").get.as[Map[String,JsValue]].get("ts_ms").get.toString

    val messageContentAsMap: Map[String, JsValue] = Json.parse(msgContent).as[Map[String,JsValue]]
    val timeStampAndOpTypeAsMap: Map[String, JsValue] = Json.obj("message_ts" -> messagetimestamp, "operation_type" -> operationType).as[Map[String,JsValue]]
    val messageContentWithTimeStampAsMap: Map[String, JsValue] = messageContentAsMap ++ timeStampAndOpTypeAsMap
    val messageContentWithTimeStamp: String = Json.stringify(Json.toJson(messageContentWithTimeStampAsMap))

    (tableName,operationType,messagetimestamp,messageContentWithTimeStamp)

  }

  // returns the required columns for pseudonimyzation,scrambing and deletion
  def getTupleOfListOfPseudoScrambleAndDeleteCols(table: String, listOfTableInfo: List[(String, List[String], List[Map[String,Any]], List[String], List[String])]) = {

    val listWithSingleTableInfo: List[(String, List[String], List[Map[String, Any]], List[String], List[String])] = listOfTableInfo.map(x => {(x._1.toLowerCase(), x._2, x._3, x._4, x._5)}).filter(y => {y._1.contains(table.toLowerCase())})

    val pseudocols: List[String] = listWithSingleTableInfo(0)._2
    val scramblecols: List[Map[String, Any]] = listWithSingleTableInfo(0)._3
    val deletecols: List[String] = listWithSingleTableInfo(0)._4

    (pseudocols, scramblecols, deletecols)

  }

  def getProcessedDataframe(df: DataFrame, columnsOfDataframe: List[String], pseudocols: List[String], scramblecols: List[Map[String,Any]], deletecols: List[String]) = {

    //Type casting the datatype of the columns to STRING
    val dfHavingAllStringTypeCols: DataFrame = columnsOfDataframe.foldLeft(df)((df, colName) => { df.withColumn(colName,col(colName).cast(StringType))})

    // Adding a CREATIONTS column to dataframe
    val dfHavingAllStringTypeCols1: DataFrame = dfHavingAllStringTypeCols
      .withColumn("message_ts",col("message_ts").cast(LongType)).withColumn("message_ts",col("message_ts")/1000)
      .withColumn("CREATIONTS",to_timestamp(col("message_ts")))

    // Pseudonimyzing the PII columns
    val pseudo = new Pseudoscramble(dfHavingAllStringTypeCols1, pseudocols, scramblecols, deletecols)
    val dfWithpseudoscrambledCols: DataFrame = pseudo.evaluate()
    dfWithpseudoscrambledCols

  }

  // Loads insert type records to delta tables
  def processInsertTypeRdds(table: String, rdd: RDD[(String,String,String,String)], listOfTableInfo: List[(String, List[String], List[Map[String,Any]], List[String], List[String])]) = {

    val (pseudocols, scramblecols, deletecols) = getTupleOfListOfPseudoScrambleAndDeleteCols(table, listOfTableInfo)

    val rddWithMsgContentOnly: RDD[String] = rdd.map(x => x._4)

    //Flattens the rdd and reads as a dataframe
    val df: DataFrame = spark.read.json(rddWithMsgContentOnly)
    val columnsOfDataframe: List[String] = df.columns.toList

    if(columnsOfDataframe.length > 0)
    {

      val dfWithpseudoscrambledCols: DataFrame = getProcessedDataframe(df, columnsOfDataframe, pseudocols, scramblecols, deletecols)
      // Derives and Adds ValidFrom and validTo columns
      addScdColumnsForInsertTypeRecords(dfWithpseudoscrambledCols: DataFrame, table: String)

    }

    else { println("No content") }

  }

  def addScdColumnsForInsertTypeRecords(dfWithInsertTypeRecords: DataFrame, table: String) = {

    val processedFilePath = processedFileRootPath + s"${table}"

    val validto = "9999-12-31T23:59:59.999"

    val finalDf: DataFrame =  dfWithInsertTypeRecords.withColumn("VALIDFROM",col("CREATIONTS")).withColumn("VALIDTO", to_timestamp(lit(validto)))

    // Writing to Delta Lake Layer
   finalDf.write.format("delta").mode(SaveMode.Append).option("header","true").save(s"${processedFileRootPath}/${table}")




    // Creating a Database on DeltaLake layer if not exists
    val createdbquery = "CREATE DATABASE IF NOT EXISTS alfa_stream"
    spark.sql(createdbquery)

    // Creating a table on DeltaLake layer if not exists
    val createTabSqlQuery = "CREATE TABLE IF NOT EXISTS alfa_stream." + table + " USING DELTA LOCATION " + "'" + processedFilePath + "'"
    spark.sql(createTabSqlQuery)

  }

  // Loads update type records to delta tables
  def processUpdateTypeRdds(table: String, rdd: RDD[(String,String,String,String)], listOfTableInfo: List[(String, List[String], List[Map[String,Any]], List[String], List[String])]) = {

    val (pseudocols, scramblecols, deletecols) = getTupleOfListOfPseudoScrambleAndDeleteCols(table, listOfTableInfo)

    val rddWithMsgContentOnly = rdd.map(x => x._4)

    //Flattens the rdd and reads as a dataframe
    val df = spark.read.json(rddWithMsgContentOnly)
    val columnsOfDataframe = df.columns.toList

    if(columnsOfDataframe.length > 0)
    {

      val dfWithpseudoscrambledCols = getProcessedDataframe(df, columnsOfDataframe, pseudocols, scramblecols, deletecols)

      listOfTableInfo.foreach(tuple => {

        if (tuple._1.toLowerCase() == table.toLowerCase())
        {
          // extracting the primary key columns
          val mandatoryKeysList = tuple._5

          // Derives and Adds ValidFrom and validTo columns
          addScdColumnsForUpdateTypeRecords(dfWithpseudoscrambledCols: DataFrame, table: String, mandatoryKeysList: List[String])
        }
      })
    }

    else{
      println("No content")
    }
  }

  def addScdColumnsForUpdateTypeRecords(dfWithUpdateTypeRecords: DataFrame, table: String, mandatoryKeysList: List[String]) = {

    val processedFilePath = processedFileRootPath + s"${table}"

    val validto = "9999-12-31T23:59:59.999"
    val dfWithValidFrom = dfWithUpdateTypeRecords.withColumn("VALIDFROM",col("CREATIONTS"))

    val finalDf =  dfWithValidFrom.withColumn("VALIDTO", to_timestamp(lit(validto)))

    // Writing to Delta Lake Layer
    //finalDf.write.format("delta").mode(SaveMode.Append).option("header","true").save(s"${processedFileRootPath}/${table}")

    val validfromless1s: String =current_date().toString()
    val deltaTable = DeltaTable.forPath(spark, processedFilePath)
    deltaTable.alias("delta")
      .merge(finalDf.alias("new"), "delta.key_field=new.key_field")
      .whenMatched("delta.VALIDTO = '9999-12-31T23:59:59.999'")
      .updateExpr(Map("VALIDTO" -> validfromless1s))
      .execute()


    // Creating a Database on DeltaLake layer if not exists
    val createdbquery = "CREATE DATABASE IF NOT EXISTS alfa_stream"
    spark.sql(createdbquery)

    // Creating a table on DeltaLake layer if not exists
    val createTabSqlQuery = "CREATE TABLE IF NOT EXISTS alfa_stream." + table + " USING DELTA LOCATION " + "'" + processedFilePath + "'"
    spark.sql(createTabSqlQuery)

    // Primary key columns list
    var keyslist: List[String] = mandatoryKeysList

    // Defining a part of update/merge query. Used in Windowing PartitionBy part of the query
    var keyListStringForPartitionBy = ""
    keyslist.foreach(column => {
      if(column != keyslist.last){
        keyListStringForPartitionBy = keyListStringForPartitionBy + s"${column}, "
      } else {
        keyListStringForPartitionBy = keyListStringForPartitionBy + s"${column} "
      }
    })

    // Defining a part of update/merge query. Used in joining condition of the query
    var wherecond = ""
    keyslist.foreach(column => { wherecond = wherecond + s"${table}.${column} = result2.${column} and " })


    try {

      // Query to upsert the data to Delta lake tables
      val mergequery = s"""with result2 as
(with result1 as
(with result as
(select distinct ${keyListStringForPartitionBy},operation_type,CREATIONTS,validfrom from alfa_stream.${table})
select * , lead(validfrom,1,"9999-12-31T23:59:59.999+0000") over (partition by ${keyListStringForPartitionBy} order by creationts asc ) as lead_validfrom from result) select *, lead_validfrom - INTERVAL 001 milliseconds as final_lead_valid from result1)
merge into alfa_stream.${table} using result2 on
${wherecond}
${table}.operation_type = result2.operation_type and
${table}.creationts = result2.creationts
when matched then update set ${table}.validto = result2.final_lead_valid;"""

      spark.sql(mergequery)

    } catch {
      case e: Exception => println("Update record has arrived for missing insert type records, " + e)

    }

  }

  // Loads delete type records to delta tables
  def processDeleteTypeRdds(table: String, rdd: RDD[(String,String,String,String)], listOfTableInfo: List[(String, List[String], List[Map[String,Any]], List[String], List[String])]) = {

    val (pseudocols, scramblecols, deletecols) = getTupleOfListOfPseudoScrambleAndDeleteCols(table, listOfTableInfo)

    val rddWithMsgContentOnly: RDD[String] = rdd.map(x => x._4)

    //Flattens the rdd and reads as a dataframe
    val df: DataFrame = spark.read.json(rddWithMsgContentOnly)
    val columnsOfDataframe: List[String] = df.columns.toList

    if(columnsOfDataframe.length > 0) {

      val dfWithpseudoscrambledCols: DataFrame = getProcessedDataframe(df, columnsOfDataframe, pseudocols, scramblecols, deletecols)

      listOfTableInfo.foreach(tuple => {

        if (tuple._1.toLowerCase() == table.toLowerCase()) {
          // extracting the primary key columns
          val mandatoryKeysList = tuple._5

          // Derives and Adds ValidFrom and validTo columns
          addScdColumnsForDeleteTypeRecords(dfWithpseudoscrambledCols: DataFrame, table: String, mandatoryKeysList: List[String])
        }
      })
    }

    else{
      println("No content")
    }

  }

  def addScdColumnsForDeleteTypeRecords(dfWithDeleteTypeRecords: DataFrame, table: String, mandatoryKeysList: List[String]) = {

    // Primary key columns list
    var keyslist: List[String] = mandatoryKeysList

    // Defining a part of update/merge query. Used in joining condition of the query
    var wherecond = ""
    keyslist.foreach(column => {
      if(column != keyslist.last){
        wherecond = wherecond + s"${table}.${column} = delete.${column} and "
      }
      else{
        wherecond = wherecond + s"${table}.${column} = delete.${column}"
      }
    }
    )

    val validto = "9999-12-31T23:59:59.999"

    val processedFilePath = processedFileRootPath + s"${table}"

    val finalDf =  dfWithDeleteTypeRecords.withColumn("VALIDFROM",col("CREATIONTS")).withColumn("VALIDTO", to_timestamp(lit(validto)))


    finalDf.persist(StorageLevel.MEMORY_ONLY)
    finalDf.persist()


    val deltaTable = DeltaTable.forPath(spark, processedFilePath)
    deltaTable.alias("delta")
      .merge(finalDf.alias("delete"), wherecond)
      .whenMatched("delta.VALIDTO ='9999-12-31T23:59:59.999+0000'")
      .updateExpr(Map("VALIDTO" -> "delete.CREATIONTS- INTERVAL 001 milliseconds","operation_type"->"d"))
    .execute()

    // Writing to Delta Lake Layer
    //finalDf.write.format("delta").mode(SaveMode.Append).option("header","true").save(s"/mnt/ddlasmldsmyle/dldata/etl_stream/processed/${table}")

    // Creating a Database on DeltaLake layer if not exists
    // val createdbquery = "CREATE DATABASE IF NOT EXISTS alfa_stream"
    // spark.sql(createdbquery)

    // Creating a table on DeltaLake layer if not exists
    // val createTabSqlQuery = "CREATE TABLE IF NOT EXISTS alfa_stream." + table + " USING DELTA LOCATION " + "'" + processedFilePath + "'"
    // spark.sql(createTabSqlQuery)



  }

  def processDf(df: DataFrame, listOfTableInfo: List[(String, List[String], List[Map[String,Any]], List[String], List[String])]) = {

    val rdd1 = df.select(col("Body")).rdd.map(x => x(0).toString)

    val rdd2 : RDD[(String,String,String,String)] = rdd1.map( x => {
      try {
        val rawDataAsMap: Map[String, JsValue] = Json.parse(x).as[Map[String,JsValue]]//(json converted into key value pairs)
        val tableName: String = rawDataAsMap.get("source").get.as[Map[String,JsValue]].get("table").get.toString
        val operationType: String =  rawDataAsMap.get("op").get.toString

        if (operationType == "c" || operationType == "u")

          getEnrichedTuple("after", rawDataAsMap, tableName, operationType)

        else getEnrichedTuple("before", rawDataAsMap, tableName, operationType)

      } catch {
        case e: Exception => {
          println("content missing, " + e)
          ("dummy_table","optype","timestamp","dummy_string")
        }
      }
    })

    // Define a mutable list which consists of tuple of rdds for each source table and table name information
    var listOfTuplesHavingTableNameAndRdd: ListBuffer[(String, RDD[(String, String, String, String)])] = new ListBuffer[(String, org.apache.spark.rdd.RDD[(String, String, String, String)])]()


    listOfSourceTables.foreach(table => {
      val rddOfSingleTable: RDD[(String, String, String, String)] = rdd2.filter(x => {x._1.contains(table)})

      // We may not get the data for all the tables in a single batch/rdd which would result in an exception.To filter out the empty rdd's,the below logic is added
      if (rddOfSingleTable.take(1).length != 0 )
      {
        listOfTuplesHavingTableNameAndRdd += ((table,rddOfSingleTable))
      }
    })

    listOfTuplesHavingTableNameAndRdd.foreach( x => {

      // persisting the rdds
      val tbl_name: String = x._1
      val rddPersist: RDD[(String, String, String, String)] = x._2
      rddPersist.persist()


      val rddWithInsertRecords: RDD[(String, String, String, String)] = rddPersist.filter(x => {x._2.contains("c")})
      val rddWithUpdateRecords: RDD[(String, String, String, String)] = rddPersist.filter(x => {x._2.contains("u")})
      val rddWithDeleteRecords: RDD[(String, String, String, String)] = rddPersist.filter(x => {x._2.contains("d")})


      processInsertTypeRdds(tbl_name, rddWithInsertRecords, listOfTableInfo)
      processUpdateTypeRdds(tbl_name, rddWithUpdateRecords, listOfTableInfo)
      processDeleteTypeRdds(tbl_name, rddWithDeleteRecords, listOfTableInfo)
      rddPersist.unpersist()

    })
  }

}
