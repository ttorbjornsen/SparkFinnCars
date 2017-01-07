package ttorbjornsen.finncars
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by torbjorn on 04.01.17.
  */
object Batch extends App {

  //  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")
//  val sc = new SparkContext(conf)
  val spark = SparkSession.
    builder().
    appName("BatchApp").
    master("local[*]").
    config("spark.cassandra.connection.host","finncars-cassandra").
    getOrCreate()
  import spark.implicits._
  //implicit val acqCarHeaderEncoder = org.apache.spark.sql.Encoders.kryo[AcqCarHeader]
  implicit val propCarEncoder = org.apache.spark.sql.Encoders.kryo[PropCar]

//  sc.setLogLevel("WARN")
//  val sqlCtx = new SQLContext(sc)
//  import sqlCtx.implicits._

  val scrapeLogDF = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "scraping_log", "keyspace" -> "finncars")).
    load()

  //GET LATEST LOAD DATE, AND SAFETY MARGIN 7 DAYS
  val scrapeLogLatestDF = scrapeLogDF.
    filter("load_date = cast('" + "2017-01-04" + "' as timestamp)")

  val finnkodeLoadDate = scrapeLogLatestDF.select("finnkode", "load_date").collect


  finnkodeLoadDate.slice(1,4).map { row =>
      val acqCarH = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
      load.
      filter("finnkode = " + row(0)).
      filter("load_date <= cast('" + row(1) + "' as timestamp)")

    //      filter("finnkode = 60070354").
//      filter("load_date <= cast('2017-01-04' as timestamp)")

      val acqCarHeaderDS = acqCarH.toDF().as[AcqCarHeader]

    val acqCarD = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
      load().
      filter("finnkode = " + row(0)).
      filter("load_date <= cast('" + row(1) + "' as timestamp)")

    val acqCarDetailsDS = acqCarD.toDF().as[AcqCarDetails]

    }
}
  //    val acqCarJoin = acqCarH.join(acqCarD, acqCarH("finnkode") <=> acqCarD("finnkodeD") &&
  //      acqCarH("load_date") <=> acqCarD("load_dateD")).
  //      drop("finnkodeD","load_dateD")

  //find latest finnkodes loaded (and therefore needs to be updated) (use scraping log table). Add safety margin (e.g. handle deleted cars)
  //for every finnkode
  //  - get header
  //  - get details
  //
  // create one record in propcar for every date in scrape log (method invoked should filter on date, not the cassandra query)

