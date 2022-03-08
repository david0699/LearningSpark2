package dataFrames

import javafx.scene.control.Separator
import org.apache.spark.sql.{DataFrame, SparkSession}

object parquet {

  /**
   *
   * @param dataFrame
   * @param path
   * @param partitions
   * @param separator
   * @param sparkSession
   */
    def writeParquet(dataFrame: DataFrame,path:String,partitions:Int,separator: String)(implicit sparkSession: SparkSession): Unit ={
      dataFrame.coalesce(partitions)
        .write
        .mode("overwrite")
        .option("sep",separator)
        .parquet(path)
    }
}
