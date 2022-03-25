package exercises.compareFiles

import dataFrames.{csv, xml}
import org.apache.spark.sql.SparkSession

object ExerciseCompareFiles1 {
  def doExerciseCompareFiles1(implicit sparkSession: SparkSession):Unit={
    import sparkSession.implicits._
    //Create DataFrame of Csv
    val path = "src/main/resources/compareFiles/people.csv"
    val dfCsv = csv.readCsvHeader(path,"true",";").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id").cache()

    //Create Dataframe of xml
    val path2 = "src/main/resources/compareFiles/people.xml"
    val dfXml = xml.readXml(path2,"person").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id").cache()

    dfCsv.printSchema()

    //Parse DataFrames to Map(K:id,V:hash of row.toString)
    val csvHashMap = dfCsv.map(r=>(r.get(0).toString.toInt,r.toString().hashCode)).collect().toMap
    val xmlHashMap = dfXml.map(r=>(r.get(0).toString.toInt,r.toString().hashCode)).collect().toMap

    //Compare Maps

    if(csvHashMap.size>xmlHashMap.size || csvHashMap.size==xmlHashMap.size) compareHashMaps(csvHashMap,xmlHashMap,"CSV")
    else compareHashMaps(xmlHashMap,csvHashMap,"XML")
  }

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
}
