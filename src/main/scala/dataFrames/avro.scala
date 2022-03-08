package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

object avro {
  def writeAvro(dataFrame: DataFrame, path:String, partitions:Int)(implicit sparkSession: SparkSession): Unit ={
    dataFrame.coalesce(partitions)
      .write
      .mode("overwrite")
      .format("avro")
      .save(path)
  }
}
