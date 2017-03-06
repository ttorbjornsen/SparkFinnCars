package ttorbjornsen.finncars
import java.io.{PrintWriter, StringWriter}
import java.time.ZoneId
import scala.reflect.io.File
import com.datastax.spark.connector.cql.CassandraConnector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructType}
import play.api.libs.json.Json
import org.apache.spark.sql.functions._




/**
  * Created by torbjorn on 12.01.17.
  */
object Export {
//  val cassandraDockerIp = "172.20.0.4"
//  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host",cassandraDockerIp)
//
////  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")
//
//  val spark = SparkSession.
//    builder().
//    appName("BatchApp").
//    master("local[*]").
////    config("spark.cassandra.connection.host",cassandraDockerIp).
//    config("spark.cassandra.connection.host","finncars-cassandra").
//    getOrCreate()
//
//  spark.sparkContext.setLogLevel("WARN")
//

  //  val spark = SparkSession.
  //    builder().
  //    appName("BatchApp").
  //    master("local[*]").
  //    getOrCreate()


//  import spark.implicits._
  def exportBtlCar(spark:SparkSession, path:String): Unit ={
    val btlCarsDF = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "btl_car", "keyspace" -> "finncars")).
      load()

    //Utility.writeToJsonFile(btlCarsDF, "/usr/output/btlcars.json")
    println("starting writing btlcars.json")
    Utility.writeToJsonFile(btlCarsDF, path)
    println("finished writing btlcars.json")

  }

  def exportAcqCarH(spark:SparkSession, path:String): Unit = {

    val acq_car_h_df = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
      load()

    println("starting writing acq_car_h.json")
    Utility.writeToJsonFile(acq_car_h_df, path)
    println("finished writing acq_car_h.json")

  }

  def exportAcqCarD(spark:SparkSession, path:String): Unit = {
    val acq_car_d_df = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
      load().
      orderBy(asc("load_date"))

    //print one file for each load date

    println("starting writing acq_car_d.json")
    Utility.writeToJsonFile(acq_car_d_df, path + "")
    println("finished writing acq_car_d.json")

  }



}
