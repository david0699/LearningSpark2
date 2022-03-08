package exercises.weblogs

import dataFrames.csv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc, regexp_extract, split, when}

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
      .sort(desc("CountStatus"))

    statusCodesDf.show()

    //Most used method
    val methodsDf = weblogsDf.select($"Method")
      .groupBy($"Method")
      .agg(count($"Method").as("CountMethod"))
      .sort(desc("CountMethod"))

    methodsDf.show()

    //
    val top1resourceDf = weblogsDf.select($"Resource")
      .groupBy($"Resource")
      .agg(count($"Resource").as("CountResource"))
      .sort(desc("CountResource"))

    top1resourceDf.show(1,false)
    
  }
}
