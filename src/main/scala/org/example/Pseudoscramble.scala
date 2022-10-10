package org.example
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.Timestamp

class Pseudoscramble(inputdf: DataFrame, pseudocols: List[String], scramblecols: List[Map[String, Any]], deletecols: List[String]) {
  def evaluate(): DataFrame = {
    var pseudodf = pseudonymize(inputdf)
    var scrambledf = scramble(pseudodf)
    var deletedf = delete(scrambledf)
    return deletedf
  }

  def pseudonymize(df: DataFrame): DataFrame = {
    var pseudodf = df
    val textHandler = new TextHandler()
    textHandler.setPasswordPhrase("123456")
    //val nbx_pseudonymize = udf(textHandler.transform(_))
    pseudocols.foreach(colname => {
      try {
        //pseudodf = pseudodf.withColumn(colname, nbx_pseudonymize(col(colname)))
      } catch {
        case _: Throwable => println(s"Unable to pseudonymize column - ${colname}")
      }
    })
    return pseudodf
  }

  def scramble(df: DataFrame): DataFrame = {
    var scrambledf = df
    try {
      scramblecols.foreach(y => {
        y.foreach(z => {
          val colname = z._1
          val scramblemap = z._2.asInstanceOf[Map[String, String]]
          val customvalue = scramblemap("custom")
          val standardvalue = scramblemap("standard")

          if(customvalue != "") {
            scrambledf = scrambledf.withColumn(colname, lit(customvalue))
          } else if(standardvalue != "") {
            if(standardvalue == "date") {
              scrambledf = scrambledf.withColumn(colname, changeDay(1)(col(colname)))
              scrambledf = scrambledf.withColumn(colname, to_date(col(colname)))
            }
          }

        })
      })
    } catch {
      case _: Throwable => println(s"Unable to scramble columns")
    }
    return scrambledf
  }

  def delete(df: DataFrame): DataFrame = {
    var deletedf = df
    deletecols.foreach(colname => {
      try {
        deletedf = deletedf.drop(colname)
      } catch {
        case _: Throwable => println(s"Unable to delete column - ${colname}")
      }
    })
    return deletedf
  }

  def changeDay(day: Int) = udf{(ts: Timestamp) =>
    import java.time.LocalDateTime
    var outvalue = ""
    try {
      val changedTS = ts.toLocalDateTime.withDayOfMonth(day)
      outvalue = Timestamp.valueOf(changedTS).toString()
    } catch {
      case _: Throwable => outvalue = ""
    }
    outvalue
  }
}
