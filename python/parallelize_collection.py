from pyspark import SparkContext

sc = sc = SparkContext('spark://master:7077', 'Tutorial')
dataset = range(1,100)

parallelized_data = sc.parallelize(dataset)

