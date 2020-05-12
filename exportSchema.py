from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
import json

appName = "Export AMRDEF Schema"
master = "local"

spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

rootTag = "AMRDEF"
rowTag = "MeterReadings"

df = spark.read.format("com.databricks.spark.xml").options(rootTag=rootTag).options(rowTag=rowTag).options(nullValue="").options(valueTag="_valueTag") \
    .load("file:////home/federicobaiocco/Downloads/primestone/Scriptstraducccion/final/exportSchema.xml")

df.printSchema()

schema = df.schema.json()

print(schema)

with open('./schema.json', 'w') as F:  
    json.dump(schema, F)

# sc = SparkContext.getOrCreate()

# # Creo un rdd temporal para exportar el schema a pickle
# temp_rdd = sc.parallelize(df.schema)
# temp_rdd.saveAsPickleFile("./amrdef-schema/schema.pickle")

