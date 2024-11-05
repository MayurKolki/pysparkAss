# Question 3: Project Allocation and Workload Analysis
# Given a DataFrame with project allocation data for multiple employees, determine each employee's
# workload level based on their hours worked in a month across various projects. Categorize
# employees as “Overloaded” if they work more than 200 hours, “Balanced” if between 100-200 hours,
# and “Underutilized” if below 100 hours. Capitalize each employee’s name, and show the aggregated
# workload status count by category


import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_date, initcap, datediff , upper
from pyspark.sql.types import DateType
os.environ["PYSPARK_PYTHON"] = "C:/Users/Admin/AppData/Local/Programs/Python/Python37/python.exe"  # or "python" depending on your Python executable

# Create Spark Session
spark_conf= SparkConf()
spark_conf.set("spark.app.name","spark-program")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

workload = [
("karthik", "ProjectA", 120),
("karthik", "ProjectB", 100),
("neha", "ProjectC", 80),
("neha", "ProjectD", 30),
("priya", "ProjectE", 110),
("mohan", "ProjectF", 40),
("ajay", "ProjectG", 70),
("vijay", "ProjectH", 150),
("veer", "ProjectI", 190),
("aatish", "ProjectJ", 60),
("animesh", "ProjectK", 95),
("nishad", "ProjectL", 210),
("varun", "ProjectM", 50),
("aadil", "ProjectN", 90)
]
workload_df = spark.createDataFrame(workload, ["name", "project", "hours"])


workload_status_df = (
    workload_df
    .groupBy("name")
    .agg(sum("hours").alias("total_hours"))
    .withColumn("name", initcap(col("name")))
    .withColumn("workload_status",
        when(col("total_hours") > 200, "Overloaded")
        .when((col("total_hours") >= 100) & (col("total_hours") <= 200), "Balanced")
        .otherwise("Underutilized"))
)

workload_count_df = (
    workload_status_df
    .groupBy("workload_status")
    .agg(count("name").alias("employee_count"))
)

workload_count_df.show()
