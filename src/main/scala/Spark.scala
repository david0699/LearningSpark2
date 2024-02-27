import org.apache.spark.sql.SparkSession
object Spark {
  def getSparkSession(name: String = "LearningSpark2", hive: Boolean = false):SparkSession ={
    if(hive){
      SparkSession
        .builder()
        .master("local[*]")
        .appName(name)
        .config("spark.some.config.option","some-value")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .master("local[*]")
        .appName(name)
        .config("spark.some.config.option","some-value")
        .getOrCreate()
    }
  }
}