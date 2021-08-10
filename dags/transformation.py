

from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Fuse_final")\
        .config('spark.driver.extraClassPath', "/opt/spark/jars/postgresql-42.2.22.jar") \
        .getOrCreate()


from pyspark.sql.functions import *
from pyspark.sql.types import *


_schema = StructType([
    StructField('region', StringType(), True),
    StructField('total_cases', IntegerType(), True),
    StructField('active_cases', IntegerType(), False),
    StructField('deaths', IntegerType(), True),
    StructField('recovered', IntegerType(), True),
    StructField('critical', IntegerType(), True),
    StructField('tested', IntegerType(), True),
    StructField('death_ratio', FloatType(), True),
    StructField('recovery_ratio', FloatType(), True)
])


df= spark.read.csv("hdfs://localhost:9000/Covid/Covid.csv", header= True, schema= _schema)



# 1. top 5 region having the most death rates.
def task1():
    task1= df.select("region","deaths").sort(col("deaths").desc())
    return (task1)

#2. List region with minimum tests.
def task2():
    task2= df.select("region","tested").sort(col("tested"))
    return (task2)

#3. Find region with the most active cases.
def task3():
    task3= df.select("region","active_cases").sort(col("active_cases").desc())
    return(task3)

#4. No. of recovered cases in regions having high active cases.
def task4():
    task4= df.select("region","active_cases","recovered").sort(col("active_cases").desc(),col("recovered"))
    return(task4)

#5. Regions with active cases greater than 1000000 (10 lakhs) and death cases less than 500000(5 lakhs)
def task5():
    task5= df.select("region","active_cases","deaths").filter((df.active_cases>1000000) & (df.deaths <500000))
    return(task5)