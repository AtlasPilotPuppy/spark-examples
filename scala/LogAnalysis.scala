// Log file contains the first 200 lines from http://ita.ee.lbl.gov/html/contrib/EPA-HTTP.html
// log file can be found at ftp://ita.ee.lbl.gov/traces/epa-http.txt.Z

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import java.util.regex.Pattern

val sc = SparkContext("spark://master:7077", "Log Analysis")
val sqlContext = new SQLContext(sc)
import sqlContext.createSchemaRDD

val log_file = sc.textFile("../data/log_file.txt")
val pattern = Pattern.compile("([^\"]\\S*|\".+?\")\\s*")
case class LogSchema(ip: String, date: String, url: String, status: String, time: String)

val tokenize_row = (row: String) => {
  val matches = pattern.matcher(row)
  var values = List[String]()
  while(matches.find)
    values = values :+ matches.group(1)
  values
}

val schema_from_row = (row: List[String]) => new LogSchema(row(0), row(1).replace("[", "").replace("]", ""),
  row(2).split(" ")(1).split("\\?")(0), row(3), row(4))

val rows  = log_file.map(tokenize_row)
val schema_rows = rows.map(schema_from_row)
schema_rows.registerAsTable("logs")
// Traffic generating ip counts
val ip_access_direct = schema_rows.map(row => (row.ip, 1)).reduceByKey(_+_).map(
  _.swap).sortByKey(ascending=false).map(_.swap).collect()
println(ip_access_direct.deep.mkString("\n"))

val url_access = sqlContext.sql("SELECT url, count(*) as counts FROM logs GROUP BY url ORDER BY counts DESC LIMIT 10").collect()

val ip_access = sqlContext.sql("SELECT ip, count(*) as counts FROM logs GROUP BY ip ORDER BY counts DESC LIMIT 10").collect()
