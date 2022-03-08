package exercises.chapter4

import dataFrames.{csv, json, parquet}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, to_date}
import org.apache.spark.sql.types.TimestampType

object ExerciseFlights {
  def doExerciseFligths()(implicit sparkSession: SparkSession): Unit ={
    val path = "src/main/resources/chapter4/departuredelays.csv"
    val fligthsDf = csv.readCsvHeader(path,"true")
    fligthsDf.show()

    import sparkSession.implicits._
    //Convert unix to timestamp
    val fligthsDateDf = fligthsDf.select($"date",$"delay",$"distance",$"origin",$"destination")
      .withColumn("date",$"date".cast(TimestampType))

    //Create temporary view
    fligthsDateDf.createOrReplaceTempView("us_delay_flights_tbl")

    //Do some querys over temporary view
    sparkSession.sql("""SELECT distance, origin, destination
     FROM us_delay_flights_tbl
     WHERE distance > 1000
     ORDER BY distance DESC""").show(10)

    sparkSession.sql("""SELECT date, delay, origin, destination
     FROM us_delay_flights_tbl
     WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
     ORDER by delay DESC""").show(10)

    sparkSession.sql("""SELECT delay, origin, destination,
     CASE
       WHEN delay > 360 THEN 'Very Long Delays'
       WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
       WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
       WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
       WHEN delay = 0 THEN 'No Delays'
       ELSE 'Early'
     END AS Flight_Delays
     FROM us_delay_flights_tbl
     ORDER BY origin, delay DESC""").show(10)

    //Read files created in chapter3
    csv.readCsvHeader("src/main/resources/output/csv/fireCalls","true").show(5)
    json.readJsonWithoutSchema("src/main/resources/output/json/fireCallsDate").show(5,false)
    parquet.readParquet("src/main/resources/output/parquet/commonCalls").show(5)



  }
}
