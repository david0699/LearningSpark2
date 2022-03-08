package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.StructType

object json {

  /**
   *
   * @param path
   * @param schema
   * @param sparkSession
   * @return
   */
  def readJsonWithSchema(path:String, schema: StructType)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession.read
      .schema(schema)
      .json(path)
  }

  /**
   *
   * @param path
   * @param sparkSession
   */
  def readJsonWithoutSchema(path:String)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession.read
      .option("inferSchema","true")
      .json(path)
  }

  def writeJson(dataFrame: DataFrame, path: String, partitions: Int): Unit ={
    dataFrame.coalesce(partitions)
      .write
      .mode("overwrite")
      .json(path)
  }
}
