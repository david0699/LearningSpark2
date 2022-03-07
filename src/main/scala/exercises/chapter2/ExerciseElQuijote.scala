package exercises.chapter2

import dataFrames.text
import org.apache.spark.sql.SparkSession

object ExerciseElQuijote {
  def doExerciseElQuijote()(implicit sparkSession: SparkSession): Unit = {
    val path: String = "src/main/resources/texts/el_quijote.txt"

    val df = text.readText(path)
    //count lines of text
    println(df.count())

    //show first 20 rows of the dataset
    //show first 5 rows of the dataset
    //show first 20 rows of the dataset without truncation
    df.show()
    df.show(5)
    df.show(false)

    //catch the firsts n rows in the DataFrame( head() and take() have the same application )
    val firstRowsWhitHead = df.head(10)
    val firstRowsWhitTake = df.take(5)

    //catch the first row of the DataFrame
    val firstRow = df.first()
  }
}
