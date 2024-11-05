# Given a DataFrame of sales agents with their total sales amounts, calculate the performance status
# based on sales thresholds: “Excellent” if sales are above 50,000, “Good” if between 25,000 and
# 50,000, and “Needs Improvement” if below 25,000. Capitalize each agent's name, and show total
# sales aggregated by performance status


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

sales = [
("karthik", 60000),
("neha", 48000),
("priya", 30000),
("mohan", 24000),
("ajay", 52000),
("vijay", 45000),
("veer", 70000),
("aatish", 23000),
("animesh", 15000),
("nishad", 8000),
("varun", 29000),
("aadil", 32000)]

sales_df = spark.createDataFrame(sales, ["name", "total_sales"])


sales_df.withColumn("name", upper(col("name"))) \
    .withColumn("performance_status",
                when(col("total_sales") > 50000, "Excellent")
                .when((col("total_sales") > 25000) & (col("total_sales") <= 50000), "Good")
                .otherwise("Needs Improvement")).show()




