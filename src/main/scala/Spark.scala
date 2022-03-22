import org.apache.spark.sql.SparkSession
object Spark {
  def getSparkSession:SparkSession ={
    SparkSession
      .builder()
      .master("local[*]")
      .appName("LearningSpark2")
      .config("spark.some.config.option","some-value")
      .getOrCreate()
  }
}