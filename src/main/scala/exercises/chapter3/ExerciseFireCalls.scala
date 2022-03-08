package exercises.chapter3

import dataFrames.{avro, csv, json, parquet}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, to_timestamp, year}
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}

object ExerciseFireCalls {
  def doExerciseFireCalls(implicit sparkSession: SparkSession): Unit ={

    val schema = StructType(List(StructField("CallNumber", IntegerType, true)
      ,StructField("UnitID", StringType, true)
      ,StructField("IncidentNumber", IntegerType, true)
      ,StructField("CallType", StringType, true)
      ,StructField("CallDate", StringType, true)
      ,StructField("WatchDate", StringType, true)
      ,StructField("CallFinalDisposition", StringType, true)
      ,StructField("AvailableDtTm", StringType, true)
      ,StructField("Address", StringType, true)
      ,StructField("City", StringType, true)
      ,StructField("Zipcode", IntegerType, true)
      ,StructField("Battalion", StringType, true)
      ,StructField("StationArea", StringType, true)
      ,StructField("Box", StringType, true)
      ,StructField("OriginalPriority", StringType, true)
      ,StructField("Priority", StringType, true)
      ,StructField("FinalPriority", IntegerType, true)
      ,StructField("ALSUnit", BooleanType, true)
      ,StructField("CallTypeGroup", StringType, true)
      ,StructField("NumAlarms", IntegerType, true)
      ,StructField("UnitType", StringType, true)
      ,StructField("UnitSequenceInCallDispatch", IntegerType, true)
      ,StructField("FirePreventionDistrict", StringType, true)
      ,StructField("SupervisorDistrict", StringType, true)
      ,StructField("Neighborhood", StringType, true)
      ,StructField("Location", StringType, true)
      ,StructField("RowID", StringType, true)
      ,StructField("Delay", FloatType, true)))

    val path = "src/main/resources/chapter3/sf-fire-calls.csv"

    val fireCallsDf = csv.readCsvSchema(path,schema)

    import sparkSession.implicits._
    //Most common type of fier calls
    val commonFireCallsDf = fireCallsDf.select($"CallType")
      .where($"CallType".isNotNull)
      .groupBy($"CallType")
      .count()
      .sort(desc("count"))
      .withColumnRenamed("count","NumberOfCalls")

    commonFireCallsDf.show(false)

    //Convert String to date format and filter by year
    val dateFormatDf = fireCallsDf.select($"CallDate",$"WatchDate",$"AvailableDtTm")
      .withColumn("CallDate",to_timestamp($"CallDate","MM/dd/yy"))
      .withColumn("WatchDate",to_timestamp($"WatchDate","MM/dd/yy"))
      .withColumn("AvailableDtTm",to_timestamp($"AvailableDtTm","MM/dd/yy hh:mm:ss a"))
      .filter(year($"CallDate").between(2005,2010))
      .orderBy(desc("CallDate"))

    dateFormatDf.show(false)

    //Write in some formats
    csv.writeCsv(fireCallsDf,"src/main/resources/output/csv/fireCalls/",1,true)
    json.writeJson(dateFormatDf,"src/main/resources/output/json/fireCallsDate/",1)
    parquet.writeParquet(commonFireCallsDf,"src/main/resources/output/parquet/commonCalls/",1,"|")
    avro.writeAvro(fireCallsDf,"src/main/resources/output/avro/fireCallsAVRO/",1)
  }

}
