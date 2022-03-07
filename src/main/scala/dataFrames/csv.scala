package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object csv {
  /**
   *
   * @param path
   * @param sparkSession
   * @return
   */
  def readCsvHeader(path: String)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession
      .read
      .option("header","true")
      .option("inferSchema","true")
      .csv(path)
  }
}
