# Log file contains the first 200 lines from http://ita.ee.lbl.gov/html/contrib/EPA-HTTP.html
# log file can be found at ftp://ita.ee.lbl.gov/traces/epa-http.txt.Z
import shlex

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row, StructField, StructType, StringType, IntegerType
 
sc = SparkContext('spark://master:7077', 'Spark SQL Intro')
sqlContext = SQLContext(sc)

log_file = sc.textFile("hdfs://master:9000/user/hdfs/log_file.log")
splits = log_file.map(lambda row: shlex.split(row))

def create_schema(row):
  ip = row[0]
  date = row[1].replace('[', '').replace(']', '')
  tokens = row[2].split(' ')
  protocol = tokens[0]
  url = tokens[1].split('?')[0]
  status = row[3]
  time = None if row[4] == '-' else int(row[4]) 
  return {'ip': ip, 'date': date, 'protocol': protocol, 'url': url, 'status': status, 'time': time}

schema_dicts = splits.map(create_schema)
log_schema = sqlContext.inferSchema(schema_dicts)

log_schema.registerAsTable('logs')
sample = sqlContext.sql('SELECT * FROM logs LIMIT 10').collect()
print sample
# find 10 most popular url's
url_access = sqlContext.sql("SELECT url, count(*) as counts FROM logs GROUP BY url ORDER BY counts DESC LIMIT 10").collect()
print url_access
# 10 highest traffic sources
ip_access = sqlContext.sql("SELECT ip, count(*) as counts FROM logs GROUP BY ip ORDER BY counts DESC LIMIT 10").collect()
print ip_access
# same operation without sparkSQL
ip_access_direct = schema_dicts.map(lambda row: (row['ip'], 1)).reduceByKey(lambda a,b: a+b).map(
 lambda r: (r[1], r[0])).sortByKey(ascending=False).map(lambda r: (r[1], r[0])).collect()
print ip_access_direct[:10]
