package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object text {
  /**
   *
   * @param path
   * @param sparkSession
   * @return
   */
  def readText(path:String)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession
      .read
      .option("sep","\n")
      .text(path)
  }
}
