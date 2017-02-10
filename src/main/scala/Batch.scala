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

  val conf = new SparkConf().
    setAppName("loadRaw").
    setMaster("local[*]").
    set("spark.cassandra.connection.host", "finncars-cassandra")
    //set("spark.cassandra.connection.host", "172.20.0.4") //scala repl test

  //  val sc = new SparkContext(conf)
  val spark = SparkSession.
    builder().
    appName("BatchApp").
    master("local[*]").
    config("spark.cassandra.connection.host", "finncars-cassandra").
    //config("spark.cassandra.connection.host","172.20.0.2"). //scala repl test
    getOrCreate()

  import spark.implicits._ //e.g. convert from data frame to dataset


/*
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
*/


//  CassandraConnector(conf).withSessionDo { session =>
//    val latestLoadDateBtl = session.
//      execute("select load_date from finncars.last_successful_load where table_name = 'prop_car_daily'")
//      .one()
//      .getTimestamp("load_date")
//  }
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
//      limit(500).
      collect

    println(deltaFinnkode.length + " finnkodes has been logged at some point. Calculate Btl. ")
    var i = 0
    //in Spark it is hard to convert custom data types (E.g. hashmap) to Dataset and keep the column names. Instead, write one-by-one prop car to cassandra (also more efficient to not use datastax spark driver when writing one record at a time)

    val buf = scala.collection.mutable.ListBuffer.empty[BtlCar]

  //TODO : Remove 0 and 1 when finished test
    val btlCars = deltaFinnkode.flatMap{ row =>
      //this returns all prop car records registered for a particular finnkode, ordered by load_date asc
      //println(row(0))
      //val finnkode = 82580680
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

      buf += btlCar
    }

  val tempDS = btlCars.filter(_ != BtlCar()).toSeq.toDS()
  tempDS.write.
    format("org.apache.spark.sql.cassandra").
    options(Map("table" -> "btl_car", "keyspace" -> "finncars")).
    mode(SaveMode.Append).save()

}

//)
//}


