import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object parallelizeCollection{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Parallelize Collection")
    val sc = new SparkContext(conf)
    
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
  }
}
