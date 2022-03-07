package exercises.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object ExerciseNameAges {
  def doExercisesNameAges()(implicit sparkSession: SparkSession): Unit ={
    //create DataFrame with name and ages
    val data = Seq(("David",20),("David",15),("Alba",23),("Yasmina",27),("Alba",18))
    val agesDF = sparkSession.createDataFrame(data).toDF("name","age")

    import sparkSession.implicits._
    //Group names and aggregate age average
    val avgAgeDf = agesDF
      .groupBy($"name")
      .agg(avg($"age").as("age average"))
      .orderBy($"age average")

    avgAgeDf.show()
  }
}
