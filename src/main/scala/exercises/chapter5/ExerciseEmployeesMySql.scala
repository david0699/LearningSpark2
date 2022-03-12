package exercises.chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{broadcast, lit, year}

import java.util.Properties

object ExerciseEmployeesMySql {
  def doExerciseEmployeesMySql()(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._

    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","password")

    val url = "jdbc:mysql://localhost:3306/employees"

    //Table employees DataFrame
    val employeesDf = sparkSession.read.jdbc(url,"employees",prop)
    employeesDf.show(5)

    //Table departments DataFrame
    val departmentsDf = sparkSession.read.jdbc(url,"departments",prop)
    departmentsDf.show()

    //Table dept_emp DataFrame
    val deptEmpDf = sparkSession.read.jdbc(url,"dept_emp",prop)
    deptEmpDf.show(5)

    //Table dept_manager DataFrame
    val deptManDf = sparkSession.read.jdbc(url,"dept_manager",prop)
    deptManDf.show(5)

    //Table titles DataFrame
    val titleDf = sparkSession.read.jdbc(url,"titles",prop)
    titleDf.show()

    //Table salaries DataFrame
    val salariesDf = sparkSession.read.jdbc(url,"salaries",prop)

    //Table managers DataFrame
    val managersDf = employeesDf.join(broadcast(deptManDf),employeesDf("emp_no")<=>deptManDf("emp_no"),"inner")
      .drop(deptManDf("emp_no"))
      .filter(year($"to_date").equalTo(9999))
      .drop("from_date","to_date")

    val managersDepartmentDf = departmentsDf.join(broadcast(managersDf),departmentsDf("dept_no")<=>managersDf("dept_no"),"inner")
      .drop(managersDf("dept_no"))

    managersDepartmentDf.show()

    //Table emp Dataframe
    val empDf = employeesDf.join(broadcast(deptEmpDf),employeesDf("emp_no")<=>deptEmpDf("emp_no"),"inner")
      .drop(deptEmpDf("emp_no"))
      .filter(year($"to_date").equalTo(9999))
      .drop("from_date","to_date")

    val empDepartmentDf = empDf.join(broadcast(departmentsDf),empDf("dept_no")<=>departmentsDf("dept_no"),"inner")
      .drop(departmentsDf("dept_no"))

    empDepartmentDf.show()

    //Table employeesDepartment DataFrame
    val employeesDepartmentDf = empDepartmentDf.union(managersDepartmentDf.select($"emp_no",$"birth_date",$"first_name",$"last_name",$"gender",$"hire_date",$"dept_no",$"dept_name"))

    //Table managers with title DataFrame
    val employeesWithTitleDf = titleDf.join(broadcast(employeesDepartmentDf),titleDf("emp_no")<=>employeesDepartmentDf("emp_no"),"inner")
      .drop(titleDf("emp_no"))
      .filter(year($"to_date").equalTo(9999))
      .drop("from_date","to_date")
      .dropDuplicates()

    employeesWithTitleDf.show()

    //Final table Dataframe
    val finalDf = employeesWithTitleDf.join(broadcast(salariesDf),employeesWithTitleDf("emp_no")<=>salariesDf("emp_no"),"inner")
      .drop(salariesDf("emp_no"))
      .filter(year($"to_date").equalTo(9999))
      .drop("from_date","to_date")
      .dropDuplicates()
      .select($"title"
        ,$"emp_no"
        ,$"first_name"
        ,$"last_name"
        ,$"birth_date"
        ,$"gender"
        ,$"hire_date"
        ,$"dept_no"
        ,$"dept_name"
        ,$"salary")

      finalDf.show()



  }
}
