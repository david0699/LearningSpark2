package exercises.chapter5

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{broadcast, desc, lit, rank, row_number, year}
import dataFrames.MySql
import org.apache.spark.sql.expressions.{Window, WindowSpec}

import java.util.Properties

object ExerciseEmployeesMySql {
  def doExerciseEmployeesMySql()(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","password")

    val url = "jdbc:mysql://localhost:3306/employees"

    //Table employees DataFrame
    val employeesDf = MySql.readEmployeesTable(url,prop)

    //Table departments DataFrame
    val departmentsDf = MySql.readDepartmentsTable(url, prop)

    //Table dept_emp DataFrame
    val deptEmpDf = MySql.readDeptEmpTable(url, prop)

    //Table dept_manager DataFrame
    val deptManDf = MySql.readDeptManagerTable(url, prop)

    //Table titles DataFrame
    val titleDf = MySql.readTitlesTable(url, prop)

    //Table salaries DataFrame
    val salariesDf = MySql.readSalariesTable(url, prop)


    //Table managers DataFrame
    val managersDf = employeesDf.join(broadcast(deptManDf),employeesDf("emp_no")<=>deptManDf("emp_no"),"inner")
      .drop(deptManDf("emp_no"))

    val managersDepartmentDf = departmentsDf.join(broadcast(managersDf),departmentsDf("dept_no")<=>managersDf("dept_no"),"inner")
      .drop(managersDf("dept_no"))

    //Table emp Dataframe
    val empDf = employeesDf.join(broadcast(deptEmpDf),employeesDf("emp_no")<=>deptEmpDf("emp_no"),"inner")
      .drop(deptEmpDf("emp_no"))

    val empDepartmentDf = empDf.join(broadcast(departmentsDf),empDf("dept_no")<=>departmentsDf("dept_no"),"inner")
      .drop(departmentsDf("dept_no"))

    //Table employeesDepartment DataFrame
    val employeesDepartmentDf = empDepartmentDf.union(managersDepartmentDf.select($"emp_no",$"birth_date",$"first_name",$"last_name",$"gender",$"hire_date",$"dept_no",$"from_date",$"to_date",$"dept_name")).toDF()
      .withColumn("row_number",row_number.over(getWindow($"emp_no",empDepartmentDf("to_date"))))
      .filter($"row_number".equalTo(1))
      .drop($"row_number")

    //Table employees with title DataFrame
    val employeesWithTitleDf = titleDf.join(broadcast(employeesDepartmentDf), titleDf("emp_no") <=> employeesDepartmentDf("emp_no"), "inner")
      .drop(titleDf("emp_no"))
      .withColumn("row_number", row_number.over(getWindow($"emp_no",titleDf("to_date"))))
      .filter($"row_number".equalTo(1))
      .drop($"row_number")
      .select($"title", employeesDepartmentDf("*"))
      .orderBy($"emp_no")

    //Final table Dataframe
    val finalDf = employeesWithTitleDf.join(broadcast(salariesDf),employeesWithTitleDf("emp_no")<=>salariesDf("emp_no"),"inner")
      .drop(salariesDf("emp_no"))
      .withColumn("row_number", row_number.over(getWindow($"emp_no",salariesDf("to_date"))))
      .filter($"row_number".equalTo(1))
      .select(employeesWithTitleDf("*"),salariesDf("salary"))
      .select($"emp_no"
        ,$"title"
        ,$"first_name"
        ,$"last_name"
        ,$"birth_date"
        ,$"gender"
        ,$"hire_date"
        ,$"dept_no"
        ,$"from_date"
        ,$"to_date"
        ,$"dept_name"
        ,$"salary")

      finalDf.show()
  }

  /**
   *
   * @param partitionCol
   * @param orderCol
   * @param sparkSession
   * @return
   */
  def getWindow(partitionCol:Column,orderCol:Column)(implicit sparkSession: SparkSession): WindowSpec ={
    Window.partitionBy(partitionCol).orderBy(orderCol.desc)
  }

}
