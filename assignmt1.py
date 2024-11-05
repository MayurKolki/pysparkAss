import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, when, avg


os.environ["PYSPARK_PYTHON"] = "C:/Users/Admin/AppData/Local/Programs/Python/Python37/python.exe"  # or "python" depending on your Python executable


sc = SparkContext("local[4]", "sparkrdd")
arr = [10,20,30,40,50,60,70,80,90]
rdd1 = sc.parallelize(arr)
avg=rdd1.mean()
print("output" ,avg)
print("adding to git ")

rdd = sc.parallelize([1, 2, 3, 4, 5])
total_sum = rdd.reduce(lambda x, y: x + y)
count = rdd.count()
average = total_sum / count
print("output of avg",average)




rdd1 = sc.textFile("C:/Users/Admin/Desktop/aws Data engineer/test_data.txt")
rdd2 = rdd1.flatMap(lambda x:x.split(" "))
rdd3=rdd2.map( lambda x: (x, 1))
rdd4=rdd3.reduceByKey( lambda x, y: x + y)
rdd5=rdd4.sortBy(lambda x:x[1],False)


for i in rdd5.collect():
   print(i)

