package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.StructType

object json {
  def readJsonWithSchema(path:String)(schema: StructType)(implicit sparkSession: SparkSession):DataFrame={
    sparkSession.read
      .schema(schema)
      .json(path)
  }

  def readJsonWithoutSchema(path:String)(implicit sparkSession: SparkSession):Unit={
    sparkSession.read
      .option("inferSchema","true")
      .json(path)
  }
}
