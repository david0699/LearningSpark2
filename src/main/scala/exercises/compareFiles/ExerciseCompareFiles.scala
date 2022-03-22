package exercises.compareFiles

import org.apache.spark.sql.{DataFrame, SparkSession}
import dataFrames.{csv, xml}


object ExerciseCompareFiles {
  def doExerciseCompareFiles(implicit sparkSession: SparkSession): Unit ={

    import sparkSession.implicits._

    //Create DataFrame of Csv
    val path = "src/main/resources/compareFiles/people.csv"
    val dfCsv = csv.readCsvHeader(path,"true",";").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id")

    //Create Dataframe of xml
    val path2 = "src/main/resources/compareFiles/people.xml"
    val dfXml = xml.readXml(path2,"person").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id")

    dfCsv.printSchema()

    val differentDf = dfCsv.except(dfXml)

    if(dfCsv.count()>dfXml.count() || dfCsv.count()==dfXml.count()) printRows(differentDf,dfXml.count(),"CSV")
    else printRows(differentDf,dfCsv.count(),"XML")

   /*
    //Parse DataFrames to Map(K:id,V:hash of row.toString)
    val csvHashMap = dfCsv.map(r=>(r.get(0).toString.toInt,r.toString().hashCode)).collect().toMap
    val xmlHashMap = dfXml.map(r=>(r.get(0).toString.toInt,r.toString().hashCode)).collect().toMap

    //Compare Maps

    if(csvHashMap.size>xmlHashMap.size || csvHashMap.size==xmlHashMap.size) compareHashMaps(csvHashMap,xmlHashMap,"CSV")
    else compareHashMaps(xmlHashMap,csvHashMap,"XML")
    */
  }

  /**
   *
   * @param df DataFrame with different columns
   * @param smallSize count of small DataFrame to compare
   * @param fileName name with file of long DataFrame to compare
   */

  def printRows(df: DataFrame,smallSize:Long,fileName:String):Unit={
    for(r<-df){
      if(r.get(0) == null) println("There's a problem!!!")
      else if(r.get(0).toString.toInt<=smallSize ) println(s"The rows with id ${r.get(0)} are not equals")
      else println(s"The row with id ${r.get(0)} only exist in file $fileName")
    }
  }

  /*
  /**
   *
   * @param LongMap Map with k:"row id" v:"hash with String of the row"-Long file
   * @param ShortMap Map with k:"row id" v:"hash with String of the row"-Short file
   * @param fileName Name of the long file
   */
  def compareHashMaps(LongMap: Map[Int, Int],ShortMap:Map[Int, Int], fileName:String): Unit ={
    for(k<-LongMap){

      if(ShortMap.getOrElse(k._1,0).equals(0)) println(s"The row with id ${k._1} only exist in file $fileName")
      else if(!k._2.equals(ShortMap.getOrElse(k._1,0))) println(s"The rows with id ${k._1} are not equals")

    }
  }

 */
}
