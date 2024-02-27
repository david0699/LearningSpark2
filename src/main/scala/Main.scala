import exercises.census.ExerciseCensus
import exercises.chapter2.{ExerciseElQuijote, ExerciseMnM}
import exercises.chapter3.{ExerciseBlogs, ExerciseFireCalls, ExerciseNameAges}
import exercises.chapter4.ExerciseFlights
import exercises.chapter5.ExerciseEmployeesMySql
import exercises.compareFiles.{ExerciseCompareFiles1, ExerciseCompareFiles2, ExerciseCompareFiles3}
import exercises.weblogs.ExerciseWeblogs
import exercises.testSeqJoin.SeqJoin
import exercises.{Shuntingyard, projectTests}
import org.apache.spark.sql.SparkSession

object Main extends App{
  implicit val sparkSession: SparkSession = Spark.getSparkSession()

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

  //SeqJoin.doSeqJoin

  //ExerciseCompareFiles1.doExerciseCompareFiles1

  //ExerciseCompareFiles2.doExerciseCompareFiles2

  //ExerciseCompareFiles3.doExerciseCompareFiles3

  projectTests.doPrueba

  //Shuntingyard.main(Array())



  sparkSession.stop()

}
