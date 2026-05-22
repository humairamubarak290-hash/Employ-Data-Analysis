from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Employee Analysis") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv(
    "C:/Users/Hp/Desktop/bdproject/employees.csv",
    header=True,
    inferSchema=True
)

df.show()
df.printSchema()

print(df.columns)

df = df.withColumnRenamed("Name", "Employee_Name") \
       .withColumnRenamed("Age", "Employee_Age") \
       .withColumnRenamed("City", "Employee_City") \
       .withColumnRenamed("Salary", "Employee_Salary") \
       .withColumnRenamed("Gender", "Employee_Gender")

df.printSchema()

df = df.drop("Employee_Gender")

df.show()
df.printSchema()

df.select("Employee_City").distinct().show()

print("Total rows:", df.count())   # 50

df.agg({"Employee_Salary": "max"}).show()

df.agg({"Employee_Salary": "min"}).show()

df.select(F.avg("Employee_Salary")).show()

df.select(
    F.count("Employee_Salary").alias("Total_Employees"),
    F.max("Employee_Salary").alias("Max_Salary"),
    F.min("Employee_Salary").alias("Min_Salary"),
    F.avg("Employee_Salary").alias("Average_Salary")
).show()

df = df.na.drop(how="all")

df = df.na.drop(subset=["Employee_Salary"])

df = df.na.fill(0)

df = df.na.drop()

df.show()

df.select("Employee_Name", "Employee_Salary").show()

df.filter(df.Employee_Salary > 50000).show()

df.groupBy("Department").count().show()

# Average salary per department
df.groupBy("Department").avg("Employee_Salary").show()

df.orderBy(df.Employee_Salary.desc()).show()

df.withColumn("Bonus", df.Employee_Salary + 5000).show()

spark.stop()
