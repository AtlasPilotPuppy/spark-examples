from pyspark.sql import S

import shlex

from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)
log_file = sc.textFile("/home/anant/projects/spark-examples/data/log_file.txt")

log_file.takeSample(True, 5)

splits = log_file.map(lambda row: shlex.split(row))
splits.cache()
splits.takeSample(True, 5)

def create_schema(row):
  ip = row[0]
  date = row[1].replace('[', '').replace(']', '')
  tokens = row[2].split(' ')
  protocol = tokens[0]
  url = tokens[1].split('?')[0]
  status = row[3]
  time = None if row[4] == '-' else int(row[4]) 
  return Row(ip=ip, date=date, protocol=protocol, url=url, status=status, time=time)
schema_dicts = splits.map(create_schema)
schema_dicts.takeSample(True, 5)

log_schema = sqlContext.inferSchema(schema_dicts)
log_schema.printSchema()
log_schema.registerAsTable('logs')

sample = sqlContext.sql('SELECT * FROM logs LIMIT 10').collect()
for row in sample:
    print row

# find 10 most popular url's
url_access = sqlContext.sql('''SELECT url, count(*) as counts FROM logs GROUP BY url
  ORDER BY counts DESC LIMIT 10''').collect()
for row in url_access:
    print row
# 10 highest traffic sources
ip_access = sqlContext.sql("SELECT ip, count(*) as counts FROM logs GROUP BY ip ORDER BY counts DESC LIMIT 10").collect()
for row in ip_access:
    print row

# same operation without sparkSQL
ip_access_direct = schema_dicts.map(lambda row: (row.ip, 1)).reduceByKey(lambda a,b: a+b).map(
 lambda r: (r[1], r[0])).sortByKey(ascending=False).map(lambda r: (r[1], r[0])).collect()
for row in ip_access[:10]:
    print row

step1 = schema_dicts.map(lambda row: (row.ip, 1))
step1.take(5)

step2 = step1.reduceByKey(lambda a,b: a+b)
step2.take(5)

step3 = step2.map(lambda r: (r[1], r[0]))
step3.take(5)

step4 = step3.sortByKey(ascending=False)
step4.take(5)

step5 = step4.map(lambda r: (r[1], r[0]))
step5.take(10)

pq_path = '/home/anant/projects/spark-examples/data/log.pq'
log_schema.saveAsParquetFile(pq_path)

parquetFile = sqlContext.parquetFile(pq_path)
parquetFile.registerTempTable('parquetTable')

ip_access = sqlContext.sql("SELECT ip, count(*) as counts FROM parquetTable GROUP BY ip ORDER BY counts DESC LIMIT 10").collect()
for row in ip_access:
    print row



