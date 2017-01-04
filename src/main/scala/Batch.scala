package ttorbjornsen.finncars
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by torbjorn on 04.01.17.
  */
object Batch {
  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlCtx = new SQLContext(sc)

  //find latest finnkodes loaded (and therefore needs to be updated) (use scraping log table). Add safety margin (e.g. handle deleted cars)
  //for every finnkode
  //  - get header
  //  - get details
  //
  // create one record in propcar for every date in scrape log (method invoked should filter on date, not the cassandra query)

  val acqCarHdrDF = sqlCtx.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
    load()

  val acqCarDetDF = sqlCtx.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
    load()





}
