import exercises.chapter2.{ExerciseElQuijote, ExerciseMnM}
import exercises.chapter3.ExerciseNameAges

object Main extends App{
  implicit val sparkSession = Spark.getSparkSession

  sparkSession.sparkContext.setLogLevel("ERROR")

  //ExerciseElQuijote.doExerciseElQuijote()

  //ExerciseMnM.doExerciseMnM()

  ExerciseNameAges.doExercisesNameAges()

  sparkSession.stop()

}
