package exercises.testSeqJoin

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SeqJoin  {
  def doSeqJoin(implicit sparkSession: SparkSession):Unit={
    val schema = StructType(Array(
      StructField("name",StringType,false),
      StructField("lastName",StringType,false),
      StructField("age",IntegerType,false),
      StructField("familyName",StringType,true),
      StructField("familyCod",IntegerType,true)
    ))

    val sonsData = Seq(Row("David","Gomez",22,"GomezSoria",1),
      Row("Alba","Gomez",20,"GomezSoria",1),
      Row("Yasmina","Salguero",27,"SalgueroDidouh",2),
      Row("Anyoli","Montero",32,"MonteroGomez",3),
      Row("Aitana","Gomez",13,"GomezCriado",4)
    )

    val fathersData = Seq(Row("Bernardo","Gomez",56,"GomezSoria",1),
      Row("Anabel","Soria",52,"GomezSoria",1),
      Row("Narciso","Salguero",72,"SalgueroDidouh",2),
      Row("Fatima","Didouh",56,"SalgueroDidouh",2),
      Row("Maritere","Gomez",57,"MonteroGomez",3),
      Row("Joselilla","Gomez",47,"GomezCriado",4)
    )

    val sonsDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(sonsData),schema)
    sonsDf.show()

    val fathersDf = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(fathersData),schema)
    fathersDf.show()

    val column = Seq("familyCod")

    import sparkSession.implicits._

    val window = Window.partitionBy("familyCod")

    sonsDf.withColumn("sum",sum($"age").over(window)).show()
    sonsDf.join(fathersDf,usingColumns = column,"left").show()
    sonsDf.join(fathersDf,usingColumns = column,"left").explain(true)
  }
}
