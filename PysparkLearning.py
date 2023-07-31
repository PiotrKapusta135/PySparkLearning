from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Learning").getOrCreate()

df = spark.read.format('com.databricks.spark.csv').\
    options(header=True,
    inferschema='true').\
    load('/home/piotrek/Projects/RF_Trading/datafile/BNBBUSD.csv')
        
    
df.show(5)
df.printSchema()
