import java.io.{PrintWriter, StringWriter}
import java.time.ZoneId
import scala.reflect.io.File
import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import play.api.libs.json.Json
import ttorbjornsen.finncars.{BtlCar, Utility, PropCar}




/**
  * Created by torbjorn on 12.01.17.
  */
object Export extends App{
//  val cassandraDockerIp = "172.20.0.2"
//  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host",cassandraDockerIp)

  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")

  val spark = SparkSession.
    builder().
    appName("BatchApp").
    master("local[*]").
    config("spark.cassandra.connection.host","finncars-cassandra").
    getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


  //  val spark = SparkSession.
  //    builder().
  //    appName("BatchApp").
  //    master("local[*]").
  //    config("spark.cassandra.connection.host","172.20.0.4").
  //    getOrCreate()


  import spark.implicits._

  val btlCars = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "btl_car", "keyspace" -> "finncars")).
    load().
    collectAsList()

  val btlCarsDF = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "btl_car", "keyspace" -> "finncars")).
    load()

  //Utility.writeToJsonFile(btlCarsDF, "/usr/output/btlcars.json")
  Utility.writeToJsonFile(btlCarsDF, "/home/torbjorn/btlcars.json")

  val acq_car_h_df = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
    load()

  Utility.writeToJsonFile(acq_car_h_df, "/usr/output/acq_car_h.json")

  val acq_car_d_df = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
    load()

  Utility.writeToJsonFile(acq_car_d_df, "/usr/output/acq_car_d.json")


}
