from pyspark import SparkContext

sc = SparkContext('spark://master:7077', 'Distributed Grep')

error_log = sc.textFile("hdfs://master:9000/user/hdfs/error.log")
error_lines = error_log.filter(lambda row: "error" in row.lower()).collect()

for line in error_lines:
    print line
