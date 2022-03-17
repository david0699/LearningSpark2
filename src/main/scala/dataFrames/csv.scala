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
  def readCsvHeader(path: String, inferSchema: String,sep: String)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession
      .read
      .option("header","true")
      .option("sep",sep)
      .option("inferSchema",inferSchema)
      .csv(path)
  }

  def readCsvHeaderIgnoreWhiteSpace(path: String, inferSchema: String,sep: String)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession
      .read
      .option("header","true")
      .option("sep",sep)
      .option("inferSchema",inferSchema)
      .option("ignoreTrailingWhiteSpace","true")
      .csv(path)
  }

  /**
   *
   * @param path
   * @param schema
   * @param sparkSession
   * @return
   */
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
  def writeCsv(dataFrame: DataFrame, path: String, partitions: Int, header:Boolean): Unit ={
    dataFrame.coalesce(partitions)
      .write
      .mode("overwrite")
      .option("header", header)
      .csv(path)
  }
}
