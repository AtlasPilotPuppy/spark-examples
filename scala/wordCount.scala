import org.apache.spark.SparkContext

val sc = SparkContext("spark://master:7077", "Parallelize Collection")

val corpus = sc.textFile("../data/pg1342.txt")

val counts = corpus.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).collect()

println(counts)


