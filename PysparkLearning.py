from pyspark.sql import SparkSession

import pyspark.sql.types as typ

import pyspark.sql.functions as fn

import matplotlib.pyplot as plt
plt.style.use('ggplot')
import bokeh.plotting as chrt


spark = SparkSession.builder.appName("Spark Learning").getOrCreate()

###############################################################################
# RDD                                                                         #
###############################################################################
df = spark.read.format('com.databricks.spark.csv').\
    options(header=True,
    inferschema='true').\
    load('/home/piotrek/Projects/RF_Trading/datafile/BNBBUSD.csv')
        
    
df.show(5)
df.printSchema()

sc = spark.sparkContext
data = sc.parallelize([('Amber', 22), ('Alfred', 23)])

data_from_file = sc.textFile('/home/piotrek/Downloads/VS14MORT.txt.gz', 4)

data = data.collect()

data_heterogenous = sc.parallelize([
('Ferrari', 'fast'),
{'Porsche': 100000},
['Spain','visited', 4504]
]).collect()

data_heterogenous[1]['Porsche']

data_from_file.take(1)

def extractInformation(row):
    import re
    import numpy as np

    selected_indices = [
         2,4,5,6,7,9,10,11,12,13,14,15,16,17,18,
         19,21,22,23,24,25,27,28,29,30,32,33,34,
         36,37,38,39,40,41,42,43,44,45,46,47,48,
         49,50,51,52,53,54,55,56,58,60,61,62,63,
         64,65,66,67,68,69,70,71,72,73,74,75,76,
         77,78,79,81,82,83,84,85,87,89
    ]

    '''
        Input record schema
        schema: n-m (o) -- xxx
            n - position from
            m - position to
            o - number of characters
            xxx - description
        1. 1-19 (19) -- reserved positions
        2. 20 (1) -- resident status
        3. 21-60 (40) -- reserved positions
        4. 61-62 (2) -- education code (1989 revision)
        5. 63 (1) -- education code (2003 revision)
        6. 64 (1) -- education reporting flag
        7. 65-66 (2) -- month of death
        8. 67-68 (2) -- reserved positions
        9. 69 (1) -- sex
        10. 70 (1) -- age: 1-years, 2-months, 4-days, 5-hours, 6-minutes, 9-not stated
        11. 71-73 (3) -- number of units (years, months etc)
        12. 74 (1) -- age substitution flag (if the age reported in positions 70-74 is calculated using dates of birth and death)
        13. 75-76 (2) -- age recoded into 52 categories
        14. 77-78 (2) -- age recoded into 27 categories
        15. 79-80 (2) -- age recoded into 12 categories
        16. 81-82 (2) -- infant age recoded into 22 categories
        17. 83 (1) -- place of death
        18. 84 (1) -- marital status
        19. 85 (1) -- day of the week of death
        20. 86-101 (16) -- reserved positions
        21. 102-105 (4) -- current year
        22. 106 (1) -- injury at work
        23. 107 (1) -- manner of death
        24. 108 (1) -- manner of disposition
        25. 109 (1) -- autopsy
        26. 110-143 (34) -- reserved positions
        27. 144 (1) -- activity code
        28. 145 (1) -- place of injury
        29. 146-149 (4) -- ICD code
        30. 150-152 (3) -- 358 cause recode
        31. 153 (1) -- reserved position
        32. 154-156 (3) -- 113 cause recode
        33. 157-159 (3) -- 130 infant cause recode
        34. 160-161 (2) -- 39 cause recode
        35. 162 (1) -- reserved position
        36. 163-164 (2) -- number of entity-axis conditions
        37-56. 165-304 (140) -- list of up to 20 conditions
        57. 305-340 (36) -- reserved positions
        58. 341-342 (2) -- number of record axis conditions
        59. 343 (1) -- reserved position
        60-79. 344-443 (100) -- record axis conditions
        80. 444 (1) -- reserve position
        81. 445-446 (2) -- race
        82. 447 (1) -- bridged race flag
        83. 448 (1) -- race imputation flag
        84. 449 (1) -- race recode (3 categories)
        85. 450 (1) -- race recode (5 categories)
        86. 461-483 (33) -- reserved positions
        87. 484-486 (3) -- Hispanic origin
        88. 487 (1) -- reserved
        89. 488 (1) -- Hispanic origin/race recode
     '''

    record_split = re\
        .compile(
            r'([\s]{19})([0-9]{1})([\s]{40})([0-9\s]{2})([0-9\s]{1})([0-9]{1})([0-9]{2})' + 
            r'([\s]{2})([FM]{1})([0-9]{1})([0-9]{3})([0-9\s]{1})([0-9]{2})([0-9]{2})' + 
            r'([0-9]{2})([0-9\s]{2})([0-9]{1})([SMWDU]{1})([0-9]{1})([\s]{16})([0-9]{4})' +
            r'([YNU]{1})([0-9\s]{1})([BCOU]{1})([YNU]{1})([\s]{34})([0-9\s]{1})([0-9\s]{1})' +
            r'([A-Z0-9\s]{4})([0-9]{3})([\s]{1})([0-9\s]{3})([0-9\s]{3})([0-9\s]{2})([\s]{1})' + 
            r'([0-9\s]{2})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([\s]{36})([A-Z0-9\s]{2})([\s]{1})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([\s]{1})([0-9\s]{2})([0-9\s]{1})' + 
            r'([0-9\s]{1})([0-9\s]{1})([0-9\s]{1})([\s]{33})([0-9\s]{3})([0-9\s]{1})([0-9\s]{1})')
    try:
        rs = np.array(record_split.split(row))[selected_indices]
    except:
        rs = np.array(['-99'] * len(selected_indices))
    return rs

data_from_file_conv = data_from_file.map(extractInformation)
data_from_file_conv.map(lambda row: row).take(1)

data_2014 = data_from_file_conv.map(lambda row: int(row[16]))
data_2014.take(10)

data_2014 = data_from_file_conv.map(lambda row: (int(row[16]), int(row[16])))
data_2014.take(10)

data_filtered = data_from_file_conv.filter(lambda row: row[5]=='F' and row[21]=='0')
data_filtered.count()

data_2014_flat = data_from_file_conv.flatMap(lambda row: (row[16], int(row[16])+1))

data_2014_flat.take(10)

data_gender = data_from_file_conv.map(lambda row: row[5]).distinct()
data_gender.take(10)

fraction = 0.1
data_sample = data_from_file_conv.sample(False, fraction, 42)

print('Original dataset: {0},\n sample: {1}'.format(
    data_from_file_conv.count(), data_sample.count()))

rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c',10)])
rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', '6'), ('d', 15)])

rdd3 = rdd1.leftOuterJoin(rdd2)
rdd3.collect()

rdd4 = rdd1.join(rdd2)
rdd4.collect()

rdd5 = rdd1.intersection(rdd2)
rdd5.collect()

rdd1 = rdd1.repartition(4)
len(rdd1.glom().collect())

data_first = data_from_file_conv.take(1)

data_take_sampled = data_from_file_conv.takeSample(False, 1, 42)
data_take_sampled

rdd1.map(lambda row: row[1]).reduce(lambda x, y: x + y)

data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 1)

works = data_reduce.reduce(lambda x, y: x / y)

data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 3)
data_reduce.reduce(lambda x, y: x/y)


data_key = sc.parallelize(
    [('a', 4),('b', 3),('c', 2),('a', 8),('d', 2),('b', 1),
     ('d', 3)], 4)

data_key.reduceByKey(lambda x, y: x + y).collect()

data_reduce.count()

data_key.countByKey().items()

data_key.saveAsTextFile('/home/piotrek/Projects/Spark/Spark/data_key.txt')

def parse_input(row):
    import re
    pattern = re.compile(r'\(\'([a-z])\', ([0-9])\)')
    row_split = pattern.split(row)
    return (row_split[1], int(row_split[2]))

data_key_rereaded = sc\
    .textFile('/home/piotrek/Projects/Spark/Spark/data_key.txt')\
    .map(parse_input)

data_key_rereaded.collect()

def f(x):
    print(x)
    
data_key.foreach(f)

###############################################################################
# Dataframes                                                                         #
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

stringJSONRDD.take(5)

swimmersJSON = spark.read.json(stringJSONRDD)

swimmersJSON.show()

swimmersJSON.createOrReplaceTempView("swimmersJSON")

spark.sql('select * from swimmersJSON').collect()
spark.sql('select * from swimmersJSON').show()

swimmersJSON.printSchema()

stringCSVRDD = sc.parallelize([
    (123, 'Katie', 19, 'brown'),
    (234, 'Michael', 22, 'green'),
    (345, 'Simone', 23, 'blue')])

schema = StructType([
    StructField("id", LongType(), True),
    StructField('name', StringType(), True),
    StructField('age', LongType(), True),
    StructField('eyeColor', StringType(), True)])

swimmers = spark.createDataFrame(stringCSVRDD, schema)
swimmers.createOrReplaceTempView('swimmers')

swimmers.printSchema()

swimmers.count()

swimmers.select('id', 'age').filter('age = 22').show()
swimmers.select(swimmers.id, swimmers.age).filter(swimmers.age==22).show()

swimmers.select('name', 'eyeColor').filter("eyeColor like 'b%'").show()

spark.sql('select count(1) from swimmers').show()

spark.sql('select id, age from swimmers where age = 22').show()

spark.sql(
    'select name, eyeColor from swimmers where eyeColor like "b%"').show()

flightPerfFilePath = '/home/piotrek/Downloads/departuredelays.csv'
airportsFilePath = '/home/piotrek/Downloads/airport-codes-na.txt'

airports = spark.read.csv(airportsFilePath, header='true', inferSchema='true', sep='\t')
airports.createOrReplaceTempView('airports')

flightPerf = spark.read.csv(flightPerfFilePath, header='true')
flightPerf.createOrReplaceTempView('FlightPerformance')

flightPerf.cache()

spark.sql('''
          select 
          a.City, 
          f.origin,
          sum(f.delay) as Delays
          from flightperformance f
          join airports a on f.origin = a.IATA
          where a.State = 'WA'
          group by a.City, f.origin
          order by sum(f.delay) desc
          '''
          ).show()

###############################################################################
#                              Data preparing                                 #
###############################################################################

df = spark.createDataFrame([
    (1, 144.5, 5.9, 33, 'M'),
    (2, 167.2, 5.4, 45, 'M'),
    (3, 124.1, 5.2, 23, 'F'),
    (4, 144.5, 5.9, 33, 'M'),
    (5, 133.2, 5.7, 54, 'F'),
    (3, 124.1, 5.2, 23, 'F'),
    (5, 129.2, 5.3, 42, 'M'),
    ], ['id', 'weight', 'height', 'age', 'gender'])

print('Count of rows: {0}'.format(df.count()))
print('Count of distinct rows: {0}'.format(df.distinct().count()))

df = df.dropDuplicates()

df.show()

print('Count of ids: {}'.format(df.count()))
print('Count of distinct ids: {0}'.format(
    df.select([c for c in df.columns if c!= 'id']).distinct().count()))

df = df.dropDuplicates(subset= [c for c in df.columns if c!= 'id'])

df.agg(
       fn.count('id').alias('count'),
       fn.countDistinct('id').alias('distinct')
       ).show()

df.withColumn('new_id', fn.monotonically_increasing_id()).show()


df_miss = spark.createDataFrame([
            (1, 143.5, 5.6, 28,
            'M', 100000),
            (2, 167.2, 5.4, 45,
            'M', None),
            (3, None , 5.2, None, None, None),
            (4, 144.5, 5.9, 33,
            'M', None),
            (5, 133.2, 5.7, 54,
            'F', None),
            (6, 124.1, 5.2, None, 'F', None),
            (7, 129.2, 5.3, 42,
            'M', 76000),
            ], ['id', 'weight', 'height', 'age', 'gender', 'income'])

df_miss.show()

df_miss.rdd.map(lambda row: (row['id'], sum([c==None for c in row]))).collect()

df_miss.where('id == 3').show()

df_miss.agg(*[
    (1- (fn.count(c) / fn.count('*')))\
    .alias(c + ' missing') for c in df_miss.columns])\
    .show()

df_miss_no_income = df_miss.select([ c for c in df_miss.columns if c!= 'income'])

df_miss_no_income.show()

df_miss_no_income.dropna(thresh=3).show()

means = df_miss_no_income.agg(
    *[fn.mean(c).alias(c)
      for c in df_miss_no_income.columns if c != 'gender'])\
    .toPandas().to_dict('records')[0]
    
means['gender'] = 'missing'

df_miss_no_income.fillna(means).show()

df_outliers = spark.createDataFrame([
(1, 143.5, 5.3, 28),
(2, 154.2, 5.5, 45),
(3, 342.3, 5.1, 99),
(4, 144.5, 5.5, 33),
(5, 133.2, 5.4, 54),
(6, 124.1, 5.1, 21),
(7, 129.2, 5.3, 42),
], ['id', 'weight', 'height', 'age'])

cols = ['weight' , 'height', 'age']
bounds = {}

for col in cols:
    quantiles = df_outliers.approxQuantile(col, [0.25, 0.75], 0.05)
    IQR = quantiles[1] - quantiles[0]
    
    bounds[col] = [
        quantiles[0] - 1.5 * IQR,
        quantiles[1] + 1.5 * IQR
        ]

outliers = df_outliers.select(*['id'] + [
    (
     (df_outliers[c] < bounds[c][0])|
     (df_outliers[c] > bounds[c][1])
     ).alias(c + '_o') for c in cols
    ])

outliers.show()

df_outliers = df_outliers.join(outliers, on='id')
df_outliers.show()
df_outliers.filter('weight_o').select('id', 'weight').show()
df_outliers.filter('age_o').select('id', 'age').show()



fraud = sc.textFile('/home/piotrek/Projects/Spark/Spark datasets/ccFraud.csv.gz')
header = fraud.first()

fraud = fraud \
    .filter(lambda row: row != header)\
    .map(lambda row: [int(elem) for elem in row.split(',')])
    
fields = [
    *[
      typ.StructField(h[1:-1], typ.IntegerType(), True) for h in header.split(',')
      ]
    ]
schema = typ.StructType(fields)
fraud_df = spark.createDataFrame(fraud, schema)

fraud_df.printSchema()

fraud_df.groupBy('gender').count().show()


numerical = ['balance', 'numTrans', 'numIntlTrans']
desc = fraud_df.describe(numerical)
desc.show()

fraud_df.agg({'balance':'skewness'}).show()

fraud_df.corr('balance', 'numTrans')

n_numerical = len(numerical)
corr = []
for i in range(0, n_numerical):
    temp = [None] * 1
    
    for j in range(i, n_numerical):
        temp.append(fraud_df.corr(numerical[i], numerical[j]))
        corr.append(temp)
    
hists = fraud_df.select('balance').rdd.flatMap(
    lambda row: row)\
    .histogram(20)
    
data = {
        'bins':hists[0][:-1],
        'freq':hists[1]}
plt.bar(data['bins'], data['freq'], width=2000)
plt.title('Histogram of \'balance\'')

b_hist = chrt.