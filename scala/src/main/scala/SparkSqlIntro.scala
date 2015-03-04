// data files can be downloaded at https://s3.amazonaws.com/hw-sandbox/tutorial1/infochimps_dataset_4778_download_16677-csv.zip

import java.io.Serializable
import java.util

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkConf

object SparkSqlIntro{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Spark SQL Intro")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.createSchemaRDD
    
    /* Spark SQL requires case classes or classes implementing the Product interface to be able to use them as table schema */
    case class DividendRecord(exchange: String, symbol: String, date: String, dividends: Double)
    def parseDividend(row: Array[String]) = new DividendRecord(row(0), row(1), row(2), row(3).toDouble)
    
    // Create RDD with file contents                                                                                                                                                                                                                                           
    val dividends = sc.textFile(args(0)) // "../data/NYSE_dividends_A.csv"
    // filter header from the dataset, then split the rows on ',' and create an rdd on class DividendRecord                                                                                                                                                                    
    val div_schema = dividends.filter(!_.startsWith("exchange")).map(_.split(",")).map(parseDividend(_))
    //Register the rdd as a table                                                                                                                                                                                                                                              
    div_schema.registerTempTable("div")
    // Try a query                                                                                                                                                                                                                                                             
    val result_select = sqlContext.sql("SELECT * FROM div").collect()
    
    val result_exchange_nyse = sqlContext.sql("SELECT * FROM div where exchange='NYSE'").collect()
    
    // Read second file                                                                                                                                                                                                                                                        
    val daily_prices = sc.textFile(args(0)) // "../data/NYSE_daily_prices_A.csv"
    
    case class DailyPricesRecord(exchange: String, symbol: String, date: String, price_open: Double, price_high: Double, price_low: Double, price_close: Double, stock_volume: Double, price_adj_close: Double)
    def parseDailyPrices(row: Array[String]) = new DailyPricesRecord(row(0), row(1), row(2), row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble, row(7).toDouble, row(8).toDouble)
    val daily_prices_schema = daily_prices.filter(!_.startsWith("exchange")).map(_.split(",")).map(parseDailyPrices(_))
    
    daily_prices_schema.registerTempTable("daily_prices")
    val daily_prices_nyse = sqlContext.sql("select * from daily_prices where exchange = 'NYSE'").collect()
    
    val join_1 = sqlContext.sql("select * from div join daily_prices on div.symbol=daily_prices.symbol LIMIT 10").collect()
    
    val group_by_dividends = sqlContext.sql("select dividends, count(*) from div where symbol='AZZ' group by dividends").collect()
    val group_by_exchange = sqlContext.sql("select exchange, count(*) from div where group by exchange").collect()
    val group_by_symbol = sqlContext.sql("select symbol, count(*) from div group by symbol").collect()
    
    val join_2 = sqlContext.sql("select * from div join daily_prices on div.symbol=daily_prices.symbol and div.date=daily_prices.date LIMIT 50").collect()
  }
}
