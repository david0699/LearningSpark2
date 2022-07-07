package exercises.compareFiles

import dataFrames.{csv, xml}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, column, concat, hash, when}

object ExerciseCompareFiles3 {
  def doExerciseCompareFiles3(implicit sparkSession: SparkSession):Unit={
    import sparkSession.implicits._

    //Create DataFrame of Csv
    val path = "src/main/resources/compareFiles/people.csv"
    val dfCsv = csv.readCsvHeader(path,"true",";").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex")
      .sort($"id")

    //Create Dataframe of xml
    val path2 = "src/main/resources/compareFiles/people.xml"
    val dfXml = xml.readXml(path2,"person").select($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex")
      .sort($"id")

    //Add hash for all rows
    val dfCsvHash = dfCsv.withColumn("hashCsv", hash(concat($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex"))).cache()
    val dfXmlHash = dfXml.withColumn("hashXml", hash(concat($"id",$"name",$"lastname",$"age",$"weight",$"height",$"sex"))).cache()

    //Filter different rows
    val differentRows = dfCsvHash.join(broadcast(dfXmlHash), usingColumns = Seq("id"), "inner")
      .filter($"hashCsv"=!=$"hashXml")

    differentRows.show()

    //Filter new rows
    val newRows = dfCsvHash.join(broadcast(dfXmlHash), usingColumns = Seq("id"), "full")
      .filter($"hashCsv".isNull || $"hashXml".isNull)

    newRows.show()
  }
}
