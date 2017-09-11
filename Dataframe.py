# -*- coding: utf-8 -*-
"""
Created on Wed Sep  6 16:13:25 2017

@author: LongNV
"""

import os

import sys

def config_spark_with_python():
    
    os.chdir("E:/Setup/workingpython")
    os.curdir
    
    if 'SPARK_HOME' not in os.environ:
        os.environ['SPARK_HOME'] = 'E:/x64/Spark/spark-2.2.0-bin-hadoop2.7'
        
    SPARK_HOME = os.environ['SPARK_HOME']
    
    sys.path.insert(0,os.path.join(SPARK_HOME,"python"))
    
    sys.path.insert(0,os.path.join(SPARK_HOME,"python","lib"))
    
    sys.path.insert(0,os.path \
                    .join(SPARK_HOME,"python","lib","pyspark.zip"))
    
    sys.path.insert(0,os.path \
                    .join(SPARK_HOME,"python","lib","py4j-0.10.4-src.zip"))
    
config_spark_with_python()

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import json
###############################################################################    
conf=SparkConf()
conf.set("spark.executor.memory", "2g")
conf.set("spark.cores.max", "4")    
conf.setAppName("longnv")  
sc = SparkContext('local', conf = conf)
#sc.stop

###############################################################################
stringJSONRDD = sc.parallelize(("""
            { "id": "123",
            "name": "Katie",
            "age": 19,
            "eyeColor": "brown"
            }""",
            """{
            "id": "234",
            "name": "Michael",
            "age": 22,
            "eyeColor": "green"
            }""",
            """{
            "id": "345",
            "name": "Simone",
            "age": 23,
            "eyeColor": "blue"
            }""")
)
    
stringJSONRDD.collect()

#convert json to dataframe
spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()

df = spark.read.json(stringJSONRDD)
df.show()

#create temp table

df.createOrReplaceTempView('df')
df.show()

#SQL Query
spark.sql('select * from df where age in (19, 22) order by age desc').show(1)

# xác định schema cho dataframe
df.printSchema()

from pyspark.sql.types import *

stringCSVRDD = sc.parallelize([
    (123, 'Katie', 19, 'brown'),
    (234, 'Michael', 22, 'green'),
    (345, 'Simone', 23, 'blue')
])
    

schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("age", LongType(), True),
    StructField("eyeColor", StringType(), True)
])


# Apply the schema to the RDD and Create DataFrame

df2 = spark.createDataFrame(stringCSVRDD, schema)
df2.show()
df2.printSchema()

#create view for df2
df2.createOrReplaceTempView("df2")

# Querying with the DataFrame API
df.count()

# filter
df.select("id", "age").filter("age = 22").show()

df.select(df.id, df.age).filter(df.age == 22).show()

# Query with SQL
# Set File Path
fightPerfPath = 'E:/x64/Spark/workingspark/learningPySpark-master/Chapter03/flight-data/airport-codes-na.txt'
airportsFilePath = 'E:/x64/Spark/workingspark/learningPySpark-master/Chapter03/flight-data/departuredelays.csv'

airports = spark.read.csv(airportsFilePath, header = 'true', inferSchema = 'true')
airports.show(10)
airports.count()
airports.createOrReplaceTempView('airports')

fightPerf = spark.read.csv(fightPerfPath, header = 'true', inferSchema = 'true')
fightPerf.createOrReplaceTempView('FlightPerformance')
fightPerf.show(10)

fightPerf.cache()
airports.cache()
spark.sql("""select * 
          from fightPerf 
          where State = 'Abbotsford'
          """).show()


spark.sql("""
        select a.City,
        f.origin,
        sum(f.delay) as Delays
        from FlightPerformance f
        join airports a
        on a.IATA = f.origin
        where a.State = 'WA'
        group by a.City, f.origin
        order by sum(f.delay) desc
        """
).show()




