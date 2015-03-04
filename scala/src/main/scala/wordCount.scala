import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object wordCount{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    
    val corpus = sc.textFile(args(0))
    
    val counts = corpus.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_).collect()

    println(counts)
  }
}

