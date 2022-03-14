package exercises.weblogs

import dataFrames.csv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc, hour, regexp_extract, split, sum, to_date, to_timestamp, udf, when, year}
import org.apache.spark.sql.types.TimestampType

object ExerciseWeblogs {
  def doExerciseWeblogs()(implicit sparkSession: SparkSession): Unit ={
    val path = "src/main/resources/weblogs/nasa_aug95.csv"
    import sparkSession.implicits._
    val rawWeblogsDf = csv.readCsvHeader(path,"true"," ")

    rawWeblogsDf.printSchema()
    rawWeblogsDf.show(false)

    //Clean raw data
    val weblogsDf = rawWeblogsDf.select($"requesting_host"
      ,$"datetime"
      ,regexp_extract($"request","(\\w*)(\\s)/",1).as("requestMethod")
      ,regexp_extract($"request","\\s(/[\\S ]* )",1).as("requestResource")
      ,regexp_extract($"request","\\s([A-Z]+/\\w*.\\d*)",1).as("requestProtocol")
      ,$"status"
      ,$"response_size")

    //Select distinct web protocols
    val protocolsDf = weblogsDf.select(when($"requestProtocol".rlike("[a-zA_Z0-9]"),$"requestProtocol").as("requestProtocol"))
      .distinct()
      .na.drop()

    protocolsDf.show()

    //Select most common status codes
    val statusDf = weblogsDf.select($"status")
      .groupBy($"status")
      .count()
      .orderBy(desc("count"))

    statusDf.show()

    //Select most common status codes
    val methodDf = weblogsDf.select($"requestMethod")
      .groupBy($"requestMethod")
      .count()
      .orderBy(desc("count"))

    methodDf.show()



    //Resource with most data transfer
    val mostDataTransferResource = weblogsDf.groupBy($"requestResource")
      .agg(sum($"response_size").as("totalByteTransfer"))
      .na.drop()
      .orderBy(desc("totalByteTransfer"))
      .limit(1)

    mostDataTransferResource.show()

    //Resource with most registers
    val mostRegistersResource = weblogsDf.groupBy($"requestResource")
      .count()
      .orderBy(desc("count"))

    mostRegistersResource.show()


    //Most traffic day
    val mostTrafficDay = weblogsDf.select(to_date($"datetime").as("Date"))
      .groupBy($"Date")
      .count()
      .orderBy(desc("count"))
      .limit(1)

    mostTrafficDay.show()


    //Most traffic hours
    val mostTrafficHours = weblogsDf.select(hour($"datetime").as("Hour"))
      .groupBy($"Hour")
      .count()
      .orderBy(desc("count")).show(24)


    //Most frequency hosts
    val mostFrequencyHosts = weblogsDf.groupBy($"requesting_host")
      .count()
      .orderBy(desc("count"))

    mostFrequencyHosts.show()


    //Errors 404 per day
    val error404Day = weblogsDf.select(to_date($"datetime").as("date"),$"status")
      .filter($"status".equalTo(404))
      .groupBy($"date")
      .agg(count($"date").as("404 Errors"))
      .orderBy(desc("404 Errors"))

    error404Day.show()
  }
}
