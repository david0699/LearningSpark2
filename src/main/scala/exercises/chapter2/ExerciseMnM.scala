package exercises.chapter2

import dataFrames.csv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, desc, max, min, round, sum}
import org.apache.spark.sql.types.IntegerType

object ExerciseMnM {
  def doExerciseMnM()(implicit sparkSession: SparkSession): Unit ={
    //Read file
    val path :String = "src/main/resources/chapter2/mnm_dataset.csv"
    val MnMdf = csv.readCsvHeader(path)
    MnMdf.printSchema()

    import sparkSession.implicits._

    //Group by state and color and count colors
    val countAggMnM = MnMdf
      .select($"State",$"Color",$"Count")
      .groupBy($"State",$"Color")
      .agg(count($"Count").as("Total"))
      .orderBy($"State",$"Color")

    countAggMnM.show()

    //Show only results of California,Texas and Nevada
    countAggMnM.filter($"State".equalTo("CA")
        || $"State".equalTo("NV")
        || $"State".equalTo("TX"))
      .show()

    //Group by state and color and aggregate max color
    val maxAggMnM = MnMdf
      .select($"State",$"Color",$"Count")
      .groupBy($"State",$"Color")
      .agg(max("Count").as("Max Count"))
      .orderBy($"State")
    maxAggMnM.show()

    //Group by state and color and aggregate some aggregations color
    val someAggMnM = MnMdf
      .select($"State",$"Color",$"Count")
      .groupBy($"State",$"Color")
      .agg(count($"Count").as("Count")
        ,max($"Count").as("Max")
        ,min($"Count").as("Min")
        ,(round(avg($"Count")*100)/100).as("Avg"))

    someAggMnM.show()




  }
}
