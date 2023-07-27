from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('db_read').getOrCreate()

data = [('Java', '2000'), ('Python', '10000')]

columns = ['langauge', 'users_count']


df = spark.createDataFrame(data).toDF(*columns)

df.show()


user = 'postgres'
pw = 'postgres'
url = 'jdbc.postgresql://postgres:postgres@localhost:5432/dataset?user=postgres&password=postgres'
df = spark.read.format('jdbc').option('url', 'jdbc:postgresql://localhost:5432/postgres')\
    .option('driver', 'org.postgres.Driver').option('dbtable', table_name)\
    .option('user','postgres').option('password', 'postgres').load()
 
table_name = 'public."BNB_BUSD_1h5Y"'

url = 'jdbc:postgresql://'+'localhost'+':5432/'+'postgres'+'?user='+'postgres'+'&password='+'postgres'
properties ={'driver': 'org.postgresql.Driver', 'password': 'postgres','user': 'postgres'}
ds = spark.read.jdbc(url=url, table=table_name, properties=properties)
