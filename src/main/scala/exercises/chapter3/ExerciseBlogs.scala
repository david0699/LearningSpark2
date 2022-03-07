package exercises.chapter3
import dataFrames.json
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, concat_ws, desc, exp, lit}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}

object ExerciseBlogs {
  def doExerciseBlogs()(implicit sparkSession: SparkSession): Unit = {
    val schema = StructType(List(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType, true), false)))

    val path = "src/main/resources/chapter3/blogs.json"

    val blogsDf = json.readJsonWithSchema(path)(schema)

    val arrayColumns = blogsDf.columns
    arrayColumns.foreach(a=>print(a + " \n"))

    import sparkSession.implicits._
    //Add column BigHits and sort by descend Id
    val BigHitsDf = blogsDf
      .withColumn("BigHits",$"Hits">10000)
      .sort(desc("Id"))
    BigHitsDf.show()

    //Create concat column
    val ConcatDf = blogsDf.withColumn("ConcatColumn",concat_ws(" ",$"Id",$"First", $"Last"))
      .select($"ConcatColumn").show()

  }
}
