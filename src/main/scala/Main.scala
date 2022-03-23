import exercises.census.ExerciseCensus
import exercises.chapter2.{ExerciseElQuijote, ExerciseMnM}
import exercises.chapter3.{ExerciseBlogs, ExerciseFireCalls, ExerciseNameAges}
import exercises.chapter4.ExerciseFlights
import exercises.chapter5.ExerciseEmployeesMySql
import exercises.compareFiles.ExerciseCompareFiles
import exercises.weblogs.ExerciseWeblogs
import exercises.testSeqJoin.SeqJoin

object Main extends App{
  implicit val sparkSession = Spark.getSparkSession

  sparkSession.sparkContext.setLogLevel("ERROR")

  //ExerciseElQuijote.doExerciseElQuijote()

  //ExerciseMnM.doExerciseMnM()

  //ExerciseNameAges.doExercisesNameAges()

  //ExerciseBlogs.doExerciseBlogs()

  //ExerciseFireCalls.doExerciseFireCalls

  //ExerciseFlights.doExerciseFligths()

  //ExerciseWeblogs.doExerciseWeblogs()

  //ExerciseEmployeesMySql.doExerciseEmployeesMySql()

  //ExerciseCensus.doExerciseCensus()

  //ExerciseCompareFiles.doExerciseCompareFiles

  sparkSession.stop()

}
