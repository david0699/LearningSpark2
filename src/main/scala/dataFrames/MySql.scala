package dataFrames

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object MySql {
  def readEmployeesTable(url:String,prop:Properties)(implicit sparkSession: SparkSession): DataFrame ={
    sparkSession.read.jdbc(url,"employees",prop)
  }

  def readDepartmentsTable(url:String,prop:Properties)(implicit sparkSession: SparkSession): DataFrame ={
    sparkSession.read.jdbc(url,"departments",prop)
  }

  def readDeptEmpTable(url:String,prop:Properties)(implicit sparkSession: SparkSession): DataFrame ={
    sparkSession.read.jdbc(url,"dept_emp",prop)
  }

  def readDeptManagerTable(url:String,prop:Properties)(implicit sparkSession: SparkSession): DataFrame ={
    sparkSession.read.jdbc(url,"dept_manager",prop)
  }

  def readTitlesTable(url:String,prop:Properties)(implicit sparkSession: SparkSession): DataFrame ={
    sparkSession.read.jdbc(url,"titles",prop)
  }

  def readSalariesTable(url:String,prop:Properties)(implicit sparkSession: SparkSession): DataFrame ={
    sparkSession.read.jdbc(url,"salaries",prop)
  }
}
