package ttorbjornsen.finncars

import java.util.TimeZone

import org.apache.log4j.{Logger,Level}
import java.time.{ZoneId, Instant, ZoneOffset, LocalDateTime}
import java.time.temporal.ChronoUnit
import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector.cql.CassandraConnector


/**
  * Created by torbjorn on 04.01.17.
  */
object Batch extends App {
  TimeZone.setDefault(TimeZone.getTimeZone("UTC")) //Cassandra stores timestamps as UTC. Ensure that UTC is used when extracting data from Cassandra into Spark
  val cassandraDockerIp = "172.20.0.5"
  val conf = new SparkConf().
    setAppName("loadRaw").
    setMaster("local[*]").
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
    set("spark.kryo.registrationRequired", "true").
    set("spark.executor.memory", "2g").
    set("spark.sql.shuffle.partitions", "2").
    registerKryoClasses(Array(classOf[PropCar], classOf[BtlCar], classOf[AcqCarHeader],classOf[AcqCarDetails],Class.forName("[[B"),Class.forName("[Lorg.apache.spark.sql.catalyst.InternalRow;"),Class.forName("org.apache.spark.sql.catalyst.expressions.UnsafeRow"),Class.forName("scala.reflect.ClassTag$$anon$1"),Class.forName("java.lang.Class"),Class.forName("scala.collection.mutable.WrappedArray$ofRef"),Class.forName("scala.math.Ordering$$anon$4"), Class.forName("org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering"), Class.forName("[Lorg.apache.spark.sql.catalyst.expressions.SortOrder;"), Class.forName("org.apache.spark.sql.catalyst.expressions.SortOrder"), Class.forName("org.apache.spark.sql.catalyst.expressions.BoundReference"), Class.forName("org.apache.spark.sql.types.TimestampType$"),Class.forName("org.apache.spark.sql.catalyst.trees.Origin"), Class.forName("org.apache.spark.sql.catalyst.expressions.Descending$") )).
    set("spark.cassandra.output.batch.size.rows","1").
    set("spark.cassandra.output.concurrent.writes", "1").
    set("spark.cassandra.connection.host", "finncars-cassandra")
    //set("spark.cassandra.connection.host", cassandraDockerIp) //scala repl test


  //  val sc = new SparkContext(conf)
  val spark = SparkSession.
    builder().
    config(conf).
    appName("BatchApp").
    master("local[*]").
    //config("spark.cassandra.connection.host", "finncars-cassandra").
//    config("spark.cassandra.connection.host",cassandraDockerIp). //scala repl test
    getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

// println(spark.sparkContext.getConf.getAll.mkString("\n"))

  import spark.implicits._ //e.g. convert from data frame to dataset

  CassandraConnector(conf).withSessionDo { session =>
    val latestLoadDate = session.
      execute("select load_date from finncars.last_successful_load where table_name = 'prop_car_daily'")
      .one()
      .getTimestamp("load_date")

    val safetyMargin = 0
    val load_dates = Utility.getDatesBetween(LocalDateTime.ofInstant(latestLoadDate.toInstant, ZoneOffset.UTC).minusDays(safetyMargin).toLocalDate, LocalDateTime.now().toLocalDate)
    println(load_dates)
    load_dates.map {
      load_date =>
        println(load_date + " to be loaded.")
        val deltaFinnkodeLoadDate = spark.read.
          format("org.apache.spark.sql.cassandra").
          options(Map("table" -> "scraping_log", "keyspace" -> "finncars")).
          load().
          filter("load_date = cast('" + load_date + "' as timestamp)").
          select("finnkode", "load_date").
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
            setString("eq", propCarFinnkode.equipment).
            setString("inf", propCarFinnkode.information).
            setInt("km", propCarFinnkode.km).
            setString("loc", propCarFinnkode.location).
            setString("pri", propCarFinnkode.price).
            setString("prop", propCarFinnkode.properties).
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



  val latestPropCarDaily = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "last_successful_load", "keyspace" -> "finncars")).
    load.
    filter("table_name = 'prop_car_daily'").
    select("load_date").
    collect

  val latestPropCarDailyDate = latestPropCarDaily(0)(0).asInstanceOf[java.util.Date]


    //val latestLoadDateBtl = new java.util.Date(2017,1,14,0,0) //repl
    val load_date = LocalDateTime.ofInstant(latestPropCarDailyDate.toInstant, ZoneOffset.UTC).toLocalDate.toString
    println(load_date + " to be loaded.")

    //get all finnkodes
    val deltaFinnkode = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "scraping_log", "keyspace" -> "finncars")).
      load().
      select("finnkode").
      distinct().
      collect

    println(deltaFinnkode.length + " finnkodes has been logged at some point. Calculate Btl. ")
    var i = 0
    //in Spark it is hard to convert custom data types (E.g. hashmap) to Dataset and keep the column names. Instead, write one-by-one prop car to cassandra (also more efficient to not use datastax spark driver when writing one record at a time)

  CassandraConnector(conf).withSessionDo { session =>
    val prepStmt = session.prepare("insert into finncars.btl_car (finnkode, antall_eiere, automatgir , cruisekontroll, deleted, deleted_date, drivstoff, effekt, farge, fylke, hengerfeste, km, kommune, last_updated, lead_time_deleted, lead_time_sold, load_date_first, load_date_latest, location, navigasjon, parkeringsensor, price_delta, price_first, price_last, regnsensor, servicehefte, skinninterior, sold, sold_date, sportsseter, sylindervolum, tilstandsrapport, title, vekt, xenon, year, url) values(:finnkode, :antall_eiere, :automatgir , :cruisekontroll, :deleted, :deleted_date, :drivstoff, :effekt, :farge, :fylke, :hengerfeste, :km, :kommune, :last_updated, :lead_time_deleted, :lead_time_sold, :load_date_first, :load_date_latest, :location, :navigasjon, :parkeringsensor, :price_delta, :price_first, :price_last, :regnsensor, :servicehefte, :skinninterior, :sold, :sold_date, :sportsseter, :sylindervolum, :tilstandsrapport, :title, :vekt, :xenon, :year, :url)")
    val btlCars = deltaFinnkode.map{ row =>

    val finnkode = row(0).asInstanceOf[Int]


      val propCarFinnkodeArray = spark.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "prop_car_daily", "keyspace" -> "finncars")).
      load.
      filter("finnkode = " + finnkode).
      orderBy("load_date").
      toDF().
      as[PropCar].
      collect

      val btlCar = if (propCarFinnkodeArray.length > 0) {
        Utility.createBtlCar(propCarFinnkodeArray)
      } else BtlCar()
      i = i + 1
      println(i + "- added to btlCar")

      //          println("after create propcar: " + row(0) + " " + System.currentTimeMillis())
    val boundStmt = prepStmt.bind().
        setInt("finnkode", btlCar.finnkode).
        setString("title", btlCar.title).
        setString("location", btlCar.location).
        setInt("year", btlCar.year).
        setInt("km", btlCar.km).
        setInt("price_first", btlCar.price_first).
        setInt("price_last", btlCar.price_last).
        setInt("price_delta", btlCar.price_delta).
        setBool("sold", btlCar.sold).
        setTimestamp("sold_date", new java.util.Date(btlCar.sold_date)).
        setInt("lead_time_sold", btlCar.lead_time_sold).
        setBool("deleted", btlCar.deleted).
        setString("deleted_date", btlCar.deleted_date).
        setInt("lead_time_deleted", btlCar.lead_time_deleted).
        setTimestamp("load_date_first", new java.util.Date(btlCar.load_date_first)).
        setTimestamp("load_date_latest", new java.util.Date(btlCar.load_date_latest)).
        setBool("automatgir", btlCar.automatgir).
        setBool("hengerfeste", btlCar.hengerfeste).
        setString("skinninterior", btlCar.skinninterior).
        setString("drivstoff", btlCar.drivstoff).
        setDouble("sylindervolum", btlCar.sylindervolum).
        setInt("effekt", btlCar.effekt).
        setBool("regnsensor", btlCar.regnsensor).
        setString("farge", btlCar.farge).
        setBool("cruisekontroll", btlCar.cruisekontroll).
        setBool("parkeringsensor", btlCar.parkeringsensor).
        setInt("antall_eiere", btlCar.antall_eiere).
        setString("kommune", btlCar.kommune).
        setString("fylke", btlCar.fylke).
        setBool("xenon", btlCar.xenon).
        setBool("navigasjon", btlCar.navigasjon).
        setBool("servicehefte", btlCar.servicehefte).
        setBool("sportsseter", btlCar.sportsseter).
        setBool("tilstandsrapport", btlCar.tilstandsrapport).
        setInt("vekt", btlCar.vekt).
        setString("last_updated", btlCar.last_updated).
        setString("url", btlCar.url)
        session.execute(boundStmt)


    }
  }


  val btlCarsDF = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "btl_car", "keyspace" -> "finncars")).
    load()

  btlCarsDF.cache
  //Utility.exportDataFrameAll(spark,btlCarsDF, "/home/torbjorn/temp/btlcar")
  Utility.exportDataFrameAll(spark,btlCarsDF, "/usr/output/btlcar/btlcar")


  val acqCarHeadersDF = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
    load()

  acqCarHeadersDF.cache
  Utility.exportDataFrameLoadDates(spark,acqCarHeadersDF, "/usr/output/acqcar/acqcarh")


  val acqCarDetailsDF = spark.read.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
    load()


  acqCarDetailsDF.cache
  Utility.exportDataFrameLoadDates(spark,acqCarDetailsDF, "/usr/output/acqcar/acqcard")

}



