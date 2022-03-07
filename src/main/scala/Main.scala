import exercises.chapter2.ExerciseElQuijote

object Main extends App{
  implicit val sparkSession = Spark.getSparkSession

  sparkSession.sparkContext.setLogLevel("ERROR")

  ExerciseElQuijote.doExerciseElQuijote()

}
