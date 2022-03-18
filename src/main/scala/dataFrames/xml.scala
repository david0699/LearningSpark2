package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object xml {
  /**
   *
   * @param path
   * @param rowTag
   * @param sparkSession
   * @return DataFrame of file XML
   */
  def readXml(path:String,rowTag:String)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession.read
      .format("com.databricks.spark.xml")
      .option("rowTag",rowTag)
      .option("order","false")
      .load(path)
  }
}
