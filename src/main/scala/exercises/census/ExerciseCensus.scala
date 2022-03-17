package exercises.census

import dataFrames.csv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{desc, length, lit, round, sum, trim}
import org.apache.spark.sql.expressions.Window

object ExerciseCensus {
  def doExerciseCensus()(implicit sparkSession: SparkSession): Unit ={

    import sparkSession.implicits._


    val df = csv.readCsvHeaderIgnoreWhiteSpace("src/main/resources/census/Rango_Edades_Seccion_202203.csv","true",";")
      .na.fill(0)
      .withColumn("DESC_DISTRITO",trim($"DESC_DISTRITO"))
      .withColumn("DESC_BARRIO",trim($"DESC_BARRIO"))


    df.show(5)
    df.printSchema()

    //Select distinct neighborhood
    val barriosDf = df.select($"DESC_BARRIO").distinct
    barriosDf.show()

    //Create table view
    df.createOrReplaceTempView("padron")

    //Select distinct neighborhood from temp view
    sparkSession.sql("SELECT DISTINCT(DESC_BARRIO) FROM padron").show()

    //Create a column with length of DESC_BARRIO
    val lengthBarrio = barriosDf.withColumn("LONGITUD",length($"DESC_BARRIO"))
    lengthBarrio.show()

    //Create a column with 5 for each record and drop the column
    val withColumn5Df = df.withColumn("5",lit("5"))
    withColumn5Df.show(5)
    withColumn5Df.drop($"5").show(5)

    //Partition table by DESC_DISTRITO, DESC_BARRIO
    val dfRepartition = df.repartition($"DESC_DISTRITO",$"DESC_BARRIO")

      /*
    Load df in cache
    group by ditrict and neighborhood ,show total of SpanishMen,SpanishWomen,ForeingMen,ForeignWomen ,order by ForeignWomen,ForeingMen
    Unload df of cache
    */
    df.persist()
    df.select($"DESC_DISTRITO",$"DESC_BARRIO",$"EspanolesHombres",$"EspanolesMujeres",$"ExtranjerosHombres",$"ExtranjerosMujeres")
      .groupBy($"DESC_DISTRITO",$"DESC_BARRIO")
      .agg(sum($"EspanolesHombres").as("SumEspanolesHombres")
        ,sum($"EspanolesMujeres").as("SumEspanolesMujeres")
        ,sum($"ExtranjerosHombres").as("SumExtranjerosHombres")
        ,sum($"ExtranjerosMujeres").as("SumExtranjerosMujeres"))
      .orderBy(desc("SumExtranjerosMujeres"),desc("SumExtranjerosHombres")).show(5)
    df.unpersist()

    /*
    Create a new DataFrame from df with DESC_DISTRITO,DESC_BARRIO,SumEspanolesHombres
    Join with df
    Do the same with window functions
    */
    val newDf = df.select($"DESC_DISTRITO",$"DESC_BARRIO",$"EspanolesHombres")
      .groupBy($"DESC_DISTRITO",$"DESC_BARRIO")
      .agg(sum($"EspanolesHombres").as("SumEspanolesHombres"))

    df.join(newDf,df("DESC_DISTRITO")<=>newDf("DESC_DISTRITO") && df("DESC_BARRIO")<=>newDf("DESC_BARRIO"))
      .drop(newDf("DESC_DISTRITO"))
      .drop(newDf("DESC_BARRIO"))
      .dropDuplicates()
      .orderBy($"COD_DISTRITO",$"COD_DIST_BARRIO")
      .show(4)

    val window = Window.partitionBy($"DESC_DISTRITO",$"DESC_BARRIO")
    df.withColumn("SumEspanolesHombres",sum($"EspanolesHombres").over(window))
      .orderBy($"COD_DISTRITO",$"COD_DIST_BARRIO")
      .show(4)




    /*
    Show sum of the EspanolesMujeres group by COD_EDAD_INT in BARAJAS,CENTRO,RETIRO
    Add percentage of women in each district with all women
    */
    val pivotDf = df.filter($"DESC_DISTRITO".equalTo("CENTRO")||$"DESC_DISTRITO".equalTo("BARAJAS")||$"DESC_DISTRITO".equalTo("RETIRO"))
      .groupBy($"COD_EDAD_INT")
      .pivot($"DESC_DISTRITO").sum("EspanolesMujeres")
      .orderBy($"COD_EDAD_INT")

    pivotDf.show()


    val percentOfTotalWomen = pivotDf.withColumn("potwBARAJAS",round($"BARAJAS"/sum($"BARAJAS").over(Window.partitionBy()),3))
      .withColumn("potwCENTRO",round($"CENTRO"/sum($"CENTRO").over(Window.partitionBy()),3))
      .withColumn("potwRETIRO",round($"RETIRO"/sum($"RETIRO").over(Window.partitionBy()),3))

    percentOfTotalWomen.show(5)

    //Write df in csv file partition by DESC_DISTRITO,DESC_BARRIO
    df.write.mode("overwrite").option("sep",";").option("header","true").partitionBy("DESC_DISTRITO","DESC_BARRIO").csv("src/main/resources/output/csv/census")

    //Write df in parquet file partition by DESC_DISTRITO,DESC_BARRIO
    df.write.mode("overwrite").option("sep",";").option("header","true").partitionBy("DESC_DISTRITO","DESC_BARRIO").parquet("src/main/resources/output/parquet/census")



  }
}
