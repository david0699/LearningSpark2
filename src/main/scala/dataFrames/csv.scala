package dataFrames

import org.apache.spark.sql.types.StructType
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

  def readCsvSchema(path: String, schema: StructType)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession
      .read
      .option("header","true")
      .schema(schema)
      .csv(path)
  }

  /**
   *
   * @param dataFrame
   * @param path
   * @param partitions
   */
  def writeCsv(dataFrame: DataFrame, path: String, partitions: Int): Unit ={
    dataFrame.coalesce(partitions)
      .write
      .mode("overwrite")
      .csv(path)
  }
}
