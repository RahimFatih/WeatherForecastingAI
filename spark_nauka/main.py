from pyspark.sql import SparkSession
import os
os.environ['JAVA_HOME'] = 'D:/Program Files/jdk-19.0.1/'
os.environ['HADOOP_HOME'] = 'D:/Program Files/spark-3.3.1-bin-hadoop3/'
os.environ['PYTHONPATH'] = 'C:/Users/Denis/AppData/Local/Programs/Python/Python38/'
os.environ['PYSPARK_PYTHON'] = 'C:/Users/Denis/AppData/Local/Programs/Python/Python38/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:/Users/Denis/AppData/Local/Programs/Python/Python38/python.exe'
spark=SparkSession.builder.master("spark://192.168.1.35:7077").appName("test").getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
wynik=rdd.count()
print('WYNIK KURWA ',wynik)