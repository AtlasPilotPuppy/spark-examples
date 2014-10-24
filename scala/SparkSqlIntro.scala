// data files can be downloaded at https://s3.amazonaws.com/hw-sandbox/tutorial1/infochimps_dataset_4778_download_16677-csv.zip                                                                                                                                            
                                                                                                                                                                                                                                                                           
import java.io.Serializable
import java.util

import org.apache.spark.sql._

val sc = new SparkContext("spark://master:7077", "Spark SQL Intro")
val sqlContext = new SQLContext(sc)
import sqlContext.createSchemaRDD

/* Spark SQL requires case classes or classes implementing the Product interface to be able to use them as table schema */
case class DividendRecord(exchange: String, symbol: String, date: String, dividends: Double)
def parseDividend(row: Array[String]) = new DividendRecord(row(0), row(1), row(2), row(3).toDouble)

// Create RDD with file contents                                                                                                                                                                                                                                           
val dividends = sc.textFile("hdfs://master:9000/user/hdfs/NYSE_dividends_A.csv")
// filter header from the dataset, then split the rows on ',' and create an rdd on class DividendRecord                                                                                                                                                                    
val div_schema = dividends.filter(!_.startsWith("exchange")).map(_.split(",")).map(parseDividend(_))
//Register the rdd as a table                                                                                                                                                                                                                                              
div_schema.registerAsTable("div")
// Try a query                                                                                                                                                                                                                                                             
val result = sqlContext.sql("SELECT * FROM div").collect()

val result = sqlContext.sql("SELECT * FROM div where exchange='NYSE'").collect()

// Read second file                                                                                                                                                                                                                                                        
val daily_prices = sc.textFile("hdfs://master:9000/user/hdfs/NYSE_daily_prices_A.csv")

case class DailyPricesRecord(exchange: String, symbol: String, date: String, price_open: Double, price_high: Double, price_low: Double, price_close: Double, stock_volume: Double, price_adj_close: Double)
def parseDailyPrices(row: Array[String]) = new DailyPricesRecord(row(0), row(1), row(2), row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble, row(7).toDouble, row(8).toDouble)
val daily_prices_schema = daily_prices.filter(!_.startsWith("exchange")).map(_.split(",")).map(parseDailyPrices(_))

daily_prices_schema.registerAsTable("daily_prices")
val daily_prices_nyse = sqlContext.sql("select * from daily_prices where exchange = 'NYSE'").collect()

val join = sqlContext.sql("select * from div join daily_prices on div.symbol=daily_prices.symbol LIMIT 10").collect()

val group_by = sqlContext.sql("select dividends, count(*) from div where symbol='AZZ' group by dividends").collect()
val group_by = sqlContext.sql("select exchange, count(*) from div where group by exchange").collect()
val group_by = sqlContext.sql("select symbol, count(*) from div group by symbol").collect()

val join = sqlContext.sql("select * from div join daily_prices on div.symbol=daily_prices.symbol and div.date=daily_prices.date LIMIT 50").collect()
