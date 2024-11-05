from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, current_date, initcap
from pyspark.sql.types import DateType

# Initialize SparkSession
spark = SparkSession.builder.appName("EmployeeStatusCheck").getOrCreate()

# Sample data
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

# Create DataFrame
employees_df = spark.createDataFrame(employees, ["name", "last_checkin"])

# Convert last_checkin column to DateType
employees_df = employees_df.withColumn("last_checkin", col("last_checkin").cast(DateType()))

# Add status column based on check-in date and capitalize names
current_date_col = current_date()
employees_df = employees_df.withColumn(
    "status",
    when((current_date_col - col("last_checkin")) <= 7, "Active").otherwise("Inactive")
).withColumn("name", initcap(col("name")))

# Show the resulting DataFrame
employees_df.show()
