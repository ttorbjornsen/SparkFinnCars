package ttorbjornsen.finncars

import java.time.LocalDateTime

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
  val scrapeLogLatestDF = scrapeLogDF.
    filter("load_date = cast('" + "2017-01-04" + "' as timestamp)")

  val finnkodeLoadDate = scrapeLogLatestDF.select("finnkode", "load_date").collect


  finnkodeLoadDate.slice(1,4).map { row =>
      val acqCarHFinnkode = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
      load.
      filter("finnkode = " + row(0)).
      filter("load_date <= cast('" + row(1) + "' as timestamp)").
      toDF().
      as[AcqCarHeader]

    //      filter("finnkode = 60070354").
//      filter("load_date <= cast('2017-01-04' as timestamp)")

    val acqCarDFinnkode = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
      load().
      filter("finnkode = " + row(0)).
      filter("load_date <= cast('" + row(1) + "' as timestamp)").
      toDF().
      as[AcqCarDetails]

    val propCarFinnkode = Utility.createPropCar(acqCarHFinnkode, acqCarDFinnkode)
    //in Spark it is hard to convert custom data types (E.g. hashmap) to Dataset and keep the column names. Instead, write one-by-one prop car to cassandra
    CassandraConnector(conf).withSessionDo{session =>
      println("before prepstmt")
      val prepStmt = session.prepare("insert into finncars.prop_car_daily (finnkode, load_date, deleted, equipment, information, km, location, price, properties, sold, title, year, url, load_time) values(:fk,:ld,:de,:eq,:inf,:km,:loc,:pri,:prop,:sold,:tit,:yr,:url,:lt)")
        // values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
//
      println("before boundstmt")
//      (:fk,:ld,:de,:eq,:inf,:km,:loc,:pri,:prop,:sold,:t,:y,:u,:lt)")

      val boundStmt = prepStmt.bind().
        setInt("fk", propCarFinnkode.finnkode).
        setTimestamp("ld", new java.util.Date(propCarFinnkode.load_date)).
        setBool("de", propCarFinnkode.deleted).
        setSet[String]("eq",propCarFinnkode.equipment).
        setString("inf", propCarFinnkode.information).
        setInt("km",propCarFinnkode.km).
        setString("loc", propCarFinnkode.location).
        setString("pri",propCarFinnkode.price).
        setMap[String,String]("prop",propCarFinnkode.properties).
        setBool("sold",propCarFinnkode.sold).
        setString("tit",propCarFinnkode.title).
        setInt("yr",propCarFinnkode.year).
        setString("url", propCarFinnkode.url).
        setTimestamp("lt", new java.util.Date(propCarFinnkode.load_time))
      //      propCarFinnkode.equipment,propCarFinnkode.information, new java.lang.Integer(propCarFinnkode.km), propCarFinnkode.location, propCarFinnkode.price, propCarFinnkode.properties, new java.lang.Boolean(propCarFinnkode.sold), propCarFinnkode.title, new java.lang.Integer(propCarFinnkode.year), propCarFinnkode.url, new java.lang.Long(propCarFinnkode.load_time))
      //val boundStmt = prepStmt.bind(new java.lang.Integer(propCarFinnkode.finnkode), new java.util.Date(propCarFinnkode.load_date), new java.lang.Boolean(propCarFinnkode.deleted),Set(propCarFinnkode.equipment),propCarFinnkode.information, new java.lang.Integer(propCarFinnkode.km), propCarFinnkode.location, propCarFinnkode.price, propCarFinnkode.properties, new java.lang.Boolean(propCarFinnkode.sold), propCarFinnkode.title, new java.lang.Integer(propCarFinnkode.year), propCarFinnkode.url, new java.util.Date(propCarFinnkode.load_time))
      println("before execute")
      session.execute(boundStmt)
      println("after execute")
    }



    }
//  println(propCarFinnkodeSeq)

//  spark.createDataset(propCarFinnkodeSeq).write.
//  format("org.apache.spark.sql.cassandra").
//  options(Map("table" -> "prop_car_daily", "keyspace" -> "finncars")).
//  save()


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




