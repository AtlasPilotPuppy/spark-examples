from pyspark import SparkContext

sc = SparkContext('spark://master:7077', 'Tutorial')
corpus = sc.textFile("../data/test.txt")

count = corpus.flatMap(lambda line: line.split(" ")).map(
    lambda word: (word, 1)).reduceByKey(lambda a,b: a+b).collect()

print count
