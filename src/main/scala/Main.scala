import exercises.chapter2.{ExerciseElQuijote, ExerciseMnM}
import exercises.chapter3.{ExerciseBlogs, ExerciseFireCalls, ExerciseNameAges}

object Main extends App{
  implicit val sparkSession = Spark.getSparkSession

  sparkSession.sparkContext.setLogLevel("ERROR")

  //ExerciseElQuijote.doExerciseElQuijote()

  //ExerciseMnM.doExerciseMnM()

  //ExerciseNameAges.doExercisesNameAges()

  //ExerciseBlogs.doExerciseBlogs()

  ExerciseFireCalls.doExerciseFireCalls

  sparkSession.stop()

}
