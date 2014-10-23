import org.apache.spark.SparkContext

val sc = SparkContext("spark://master:7077", "Parallelize Collection")

val corpus = sc.textFile("hdfs://master:9000/user/hdfs/test.txt")

val counts = corpus.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).collect()

println(counts)


