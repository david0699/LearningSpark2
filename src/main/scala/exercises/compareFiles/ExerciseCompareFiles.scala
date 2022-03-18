package exercises.compareFiles

import org.apache.spark.sql.SparkSession
import dataFrames.{csv, xml}
import org.json4s.JsonDSL.int2jvalue

import scala.collection.mutable.HashMap

object ExerciseCompareFiles {
  def doExerciseCompareFiles(implicit sparkSession: SparkSession): Unit ={

    import sparkSession.implicits._

    //Create DataFrame of Csv
    val path = "src/main/resources/compareFiles/people.csv"
    val dfCsv = csv.readCsvHeader(path,"true",";").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id")

    //Create Dataframe of xml
    val path2 = "src/main/resources/compareFiles/people.xml"
    val dfXml = xml.readXml(path2,"person").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex").sort($"id")

    //Parse DataFrames to Map(K:id,V:hash of row.toString)

    val csvHashMap = dfCsv.map(r=>(r.get(0).toString.toInt,r.toString().hashCode)).collect().toMap
    val xmlHashMap = dfXml.map(r=>(r.get(0).toString.toInt,r.toString().hashCode)).collect().toMap

    //Compare Maps
    for(k<-csvHashMap){
      if(!k._2.equals(xmlHashMap.getOrElse(k._1,0))) println(s"The rows with id: ${k._1} are not equals")
    }

  }
}
