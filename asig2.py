import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

# os.environ["PYSPARK_PYTHON"] = "C:/Users/Karthik Kondpak/AppData/Local/Programs/Python/Python37/python.exe"  # or "python" depending on your Python executable
os.environ["PYSPARK_PYTHON"] = "C:/Users/Admin/AppData/Local/Programs/Python/Python37/python.exe"  # or "python" depending on your Python executable

# Create Spark Session
spark = SparkSession.builder \
    .appName("Example") \
    .master("local[*]") \
    .getOrCreate()


schema="id int,Name string,Age int"

df = spark.read \
    .format("csv") \
    .option("header", True) \
    .schema(schema) \
    .option("path", "C:/Users/Admin/Documents/info.csv") \
    .load()

df.show()


df.printSchema()