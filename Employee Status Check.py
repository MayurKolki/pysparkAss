
# Create a DataFrame that lists employees with names and their work status. For each employee,
# determine if they are “Active” or “Inactive” based on the last check-in date. If the check-in date is
# within the last 7 days, mark them as "Active"; otherwise, mark them as "Inactive." Ensure the first
# letter of each name is capitalized

import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, sum, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.types import DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_date, initcap, datediff
from pyspark.sql.types import DateType
os.environ["PYSPARK_PYTHON"] = "C:/Users/Admin/AppData/Local/Programs/Python/Python37/python.exe"  # or "python" depending on your Python executable

# Create Spark Session
spark_conf= SparkConf()
spark_conf.set("spark.app.name","spark-program")
spark_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=spark_conf).getOrCreate()

employees = [
("karthik", "2024-11-01"),
("neha", "2024-10-20"),
("priya", "2024-10-28"),
("mohan", "2024-11-02"),
("ajay", "2024-09-15"),
("vijay", "2024-10-30"),
("veer", "2024-10-25"),
("aatish", "2024-10-10"),
("animesh", "2024-10-15"),
("nishad", "2024-11-01"),
("varun", "2024-10-05"),
("aadil", "2024-09-30")
]
employees_df = spark.createDataFrame(employees, ["name", "last_checkin"])
employees_df = employees_df.withColumn("last_checkin", col("last_checkin").cast(DateType()))

employees_df = employees_df.withColumn(
    "status",
    when(datediff(current_date(), col("last_checkin")) <= 7, "Active").otherwise("Inactive")
).withColumn("name", initcap(col("name")))

employees_df.show()
employees_df.printSchema()