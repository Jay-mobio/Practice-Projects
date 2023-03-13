from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JoinExample").getOrCreate()

employees = spark.read.csv("B:\Python\employees.csv",header=True,inferSchema=True)

departments = spark.read.csv("B:\Python\department.csv",header=True,inferSchema=True)

employee_dept = employees.join(departments,employees.emp_dept_id==departments.dept_id,"fullouter")

employee_dept.show()

spark.stop()
