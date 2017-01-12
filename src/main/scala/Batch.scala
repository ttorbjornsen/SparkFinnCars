package ttorbjornsen.finncars

import java.time.{Instant, ZoneOffset, LocalDateTime}

import com.datastax.driver.core.LocalDate
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Logger,Level}
import com.datastax.spark.connector.cql.CassandraConnector

/**
  * Created by torbjorn on 04.01.17.
  */
object Batch extends App {

    val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")
//  val sc = new SparkContext(conf)
  val spark = SparkSession.
    builder().
    appName("BatchApp").
    master("local[*]").
    config("spark.cassandra.connection.host","finncars-cassandra").
    getOrCreate()

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
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


  CassandraConnector(conf).withSessionDo{session =>
    val latestLoadDate = session.
      execute("select load_date from finncars.last_successful_load where table_name = 'prop_car_daily'")
      .one()
      .getTimestamp("load_date")

    val safetyMargin = 1
    val load_dates = Utility.getDatesBetween(LocalDateTime.ofInstant(latestLoadDate.toInstant,ZoneOffset.UTC).minusDays(safetyMargin).toLocalDate, LocalDateTime.now().toLocalDate)
    println(load_dates)
    load_dates.map{
      load_date =>
        println(load_date + " to be loaded.")
        val deltaFinnkodeLoadDate = spark.read.
          format("org.apache.spark.sql.cassandra").
          options(Map("table" -> "scraping_log", "keyspace" -> "finncars")).
          load().
          filter("load_date = cast('" + load_date + "' as timestamp)").
          select("finnkode","load_date").
          collect

        println(deltaFinnkodeLoadDate.length + " records ")
        var i = 0
        //in Spark it is hard to convert custom data types (E.g. hashmap) to Dataset and keep the column names. Instead, write one-by-one prop car to cassandra (also more efficient to not use datastax spark driver when writing one record at a time)
        val prepStmt = session.prepare("insert into finncars.prop_car_daily (finnkode, load_date, deleted, equipment, information, km, location, price, properties, sold, title, year, url, load_time) values(:fk,:ld,:de,:eq,:inf,:km,:loc,:pri,:prop,:sold,:tit,:yr,:url,:lt)")



        deltaFinnkodeLoadDate.map { row =>
          //MOST LIKELY SUFFICIENT TO EXTRACT == for header and detail, but may be ok if used for error handling (detail record missing e.g.)
          val acqCarHFinnkode = spark.read.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
            load.
            filter("finnkode = " + row(0)).
            filter("load_date <= cast('" + row(1) + "' as timestamp)").
            toDF().
            as[AcqCarHeader]


          val acqCarDFinnkode = spark.read.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
            load().
            filter("finnkode = " + row(0)).
            filter("load_date <= cast('" + row(1) + "' as timestamp)").
            toDF().
            as[AcqCarDetails]
//          println("before create propcar : " + row(0) + " " + System.currentTimeMillis())
          val propCarFinnkode = Utility.createPropCar(acqCarHFinnkode, acqCarDFinnkode)
//          println("after create propcar: " + row(0) + " " + System.currentTimeMillis())
          val boundStmt = prepStmt.bind().
            setInt("fk", propCarFinnkode.finnkode).
            setTimestamp("ld", new java.util.Date(propCarFinnkode.load_date)).
            setBool("de", propCarFinnkode.deleted).
            setSet[String]("eq", propCarFinnkode.equipment).
            setString("inf", propCarFinnkode.information).
            setInt("km", propCarFinnkode.km).
            setString("loc", propCarFinnkode.location).
            setString("pri", propCarFinnkode.price).
            setMap[String, String]("prop", propCarFinnkode.properties).
            setBool("sold", propCarFinnkode.sold).
            setString("tit", propCarFinnkode.title).
            setInt("yr", propCarFinnkode.year).
            setString("url", propCarFinnkode.url).
            setTimestamp("lt", new java.util.Date(propCarFinnkode.load_time))

//          println("before execute: " + " " + row(0))
          session.execute(boundStmt)
//          println("after execute: " + " " + row(0))
          i = i + 1
          println(i + " records written for " + load_date)
        }
        println("insert " + load_date + " into last_successful_load for table prop_car_daily")
        session.execute("INSERT INTO finncars.last_successful_load(table_name,load_date) VALUES('prop_car_daily', '" + load_date + "')")

      }

    }
  }



