import org.apache.spark.SparkContext

val sc = SparkContext("spark://master:7077", "Parallelize Collection")

val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
