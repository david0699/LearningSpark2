package exercises.compareFiles

import org.apache.spark.sql.{DataFrame, SparkSession}
import dataFrames.{csv, xml}


object ExerciseCompareFiles2 {
  def doExerciseCompareFiles2(implicit sparkSession: SparkSession): Unit ={

    import sparkSession.implicits._

    //Create DataFrame of Csv
    val path = "src/main/resources/compareFiles/people.csv"
    val dfCsv = csv.readCsvHeader(path,"true",";").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id").cache()

    //Create Dataframe of xml
    val path2 = "src/main/resources/compareFiles/people.xml"
    val dfXml = xml.readXml(path2,"person").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id").cache()

    dfCsv.printSchema()

    val differentDf = dfCsv.except(dfXml)

    val sizeCsv = dfCsv.count()
    val sizeXml = dfXml.count()

    if(sizeCsv>sizeXml || sizeCsv==sizeXml) printRows(differentDf,sizeXml,"CSV")
    else printRows(differentDf,sizeCsv,"XML")

  }

  /**
   *
   * @param df DataFrame with different columns
   * @param smallSize count of small DataFrame to compare
   * @param fileName name with file of long DataFrame to compare
   */

  def printRows(df: DataFrame,smallSize:Long,fileName:String):Unit={
    df.foreach(r=>{
      if(r.get(0) == null) println("There's a problem!!!")
      else if (r.get(0).toString.toInt<=smallSize) println(s"The rows with id ${r.get(0)} are not equals")
      else println(s"The row with id ${r.get(0)} only exist in file $fileName")

    })
  }
}
