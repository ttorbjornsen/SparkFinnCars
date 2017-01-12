import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ttorbjornsen.finncars.PropCar


/**
  * Created by torbjorn on 12.01.17.
  */
object Export extends App{
  val spark = SparkSession.
    builder().
    appName("BatchApp").
    master("local[*]").
    config("spark.cassandra.connection.host","finncars-cassandra").
    getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  import spark.implicits._

  val latestLoadDate = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "last_successful_load", "keyspace" -> "finncars")).
    load()

  val propCarDF = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "prop_car_daily", "keyspace" -> "finncars")).
    load.
    filter("load_date = cast('" + latestLoadDate.first.get(1) + "' as timestamp)").
    toDF()

  saveTo propCarDF.rdd


    as[PropCar]

  val propCar = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "prop_car_daily", "keyspace" -> "finncars")).
    load.
    filter("load_date <= cast('" +  + "' as timestamp)").
    toDF().
    as[PropCar]

}
