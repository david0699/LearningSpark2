package exercises.weblogs

import dataFrames.csv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc, regexp_extract, split, to_timestamp, udf, when}
import org.apache.spark.sql.types.TimestampType

object ExerciseWeblogs {
  def doExerciseWeblogs()(implicit sparkSession: SparkSession): Unit ={
    val path = "src/main/resources/weblogs/weblog.csv"
    import sparkSession.implicits._
    val rawWeblogsDf = csv.readCsvHeader(path,"true")

    rawWeblogsDf.printSchema()
    rawWeblogsDf.show(false)

    val weblogsDf = rawWeblogsDf.select($"IP"
      ,$"Time"
      ,$"URL"
      ,split($"URL"," ").getItem(0).as("Method")
      ,split($"URL"," ").getItem(1).as("Resource")
      ,split($"URL"," ").getItem(2).as("Protocol")
      ,$"Staus")
      .withColumnRenamed("Staus","Status")

    weblogsDf.show()

    //Group different protocols
    val protocolsDf = weblogsDf
      .groupBy($"Protocol")
      .agg($"Protocol")
      .select($"Protocol")
      .where($"Protocol".isNotNull)

    protocolsDf.show()

    //Most used status codes
    val statusCodesDf = weblogsDf.select($"Status")
      .groupBy($"Status")
      .agg(count($"Status").as("CountStatus"))
      .filter($"Status".rlike("[0-9]$"))
      .sort(desc("CountStatus"))

    statusCodesDf.show()

    //Most used method
    val methodsDf = weblogsDf.select($"Method")
      .groupBy($"Method")
      .agg(count($"Method").as("CountMethod"))
      .filter($"Method".rlike("[A-Z]"))
      .sort(desc("CountMethod"))

    methodsDf.show()

    //Most used resource
    val top1Row = weblogsDf.select($"Resource")
      .groupBy($"Resource")
      .agg(count($"Resource").as("CountResource"))
      .sort(desc("CountResource"))
      .first()

    val top1Df = sparkSession.sparkContext
      .parallelize(Seq(top1Row))
      .map(row => (row.getString(0),row.getLong(1))).toDF("Resource","CountResource")

    top1Df.show()

    val month_map = Map ("Jan" -> 1, "Feb" -> 2, "Mar" -> 3, "Apr" -> 4, "May" -> 5, "Jun" -> 6,
      "Jul" -> 7, "Aug" -> 8,"Sep" -> 9, "Oct" -> 10, "Nov" -> 11, "Dec" -> 12)



  }
}
