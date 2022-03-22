package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object MySql {

  def readTable(table:String)(implicit sparkSession: SparkSession): DataFrame ={
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","password")

    sparkSession.read.jdbc("jdbc:mysql://localhost:3306/employees",table,prop)
  }
}
