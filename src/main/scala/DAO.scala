package ttorbjornsen.finncars
/**
  * Created by torbjorn.torbjornsen on 11.07.2016.
  */
import java.time.{Instant, LocalDate, LocalTime}
import java.util.HashMap
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.datastax.spark.connector._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}




//class DAO (_hc: SQLContext, _csc:CassandraSQLContext) extends java.io.Serializable{
//  import _hc.implicits._
//
//  /* returns the current price, but if the car is marked as sold, then the price from before it was sold is retrieved.  */
//  // tbd : derive load_date from load time and reduce number of parameters
//  def getLastPrice(price:String, url:String, load_date:String, load_time:Long):Int = {
//    if (price != "Solgt") {
//      val parsedPrice = price.replace(",-","").replace(" ","").replace("\"", "")
//      if (parsedPrice.forall(_.isDigit)) parsedPrice.toInt else -1 //price invalid
//    } else {
//
//      val prevAcqCarHeaderNotSold = _csc.sparkContext.cassandraTable[AcqCarHeader]("finncars", "acq_car_header").
//        where("url = ?", url).
//        where("load_time <= ?", new java.util.Date(load_time)).
//        filter(row => row.price != "Solgt").
//        collect
//      if (prevAcqCarHeaderNotSold.length > 0) {
//        val acqCarHeader = prevAcqCarHeaderNotSold.maxBy(_.load_time)
//        val parsedPrice = acqCarHeader.price.replace(",-","").replace(" ","").replace("\"", "")
//        if (parsedPrice.forall(_.isDigit)) {
//          parsedPrice.toInt
//        } else -1 //price invalid
//      } else -1 //previous car price not found
//    }
//  }
//
//  def getPropCarDateRange(dateStart:LocalDate, dateEnd:LocalDate):RDD[(String,PropCar)] = {
//    //val dateStart = LocalDate.now().plusDays(-365)
//    //val dateEnd = LocalDate.now
//    val dateKeys = Utility.getDatesBetween(dateStart, dateEnd)
//
//    val propCarPartitions = dateKeys.map(date => _csc.sparkContext.cassandraTable[PropCar]("finncars", "prop_car_daily").
//      select("url", "load_date", "finnkode", "title", "location", "year", "km", "price", "properties", "equipment", "information", "sold", "deleted", "load_time").
//      where("load_date = ?", date))
//
//    val propCarPairRDD = _csc.sparkContext.union(propCarPartitions).map { row =>
//      (row.url, row)
//    }
//    propCarPairRDD
//  }
//
//
//  def getLatestLoadDate(tableName:String):LocalDate = {
//    val currentDay = LocalDate.now()
//    val startLoadDate = currentDay.minusDays(Utility.Constants.ETLSafetyMargin)
//    val daysBetween = Utility.Constants.ETLSafetyMargin
//
//    val listOfDays = ListBuffer[String]()
//    for (i <- (0 to daysBetween).reverse) {
//      listOfDays += (startLoadDate.plusDays(i).toString)
//    }
//
//    val datesWithRecords = listOfDays.map { date =>
//      _csc.sparkContext.cassandraTable("finncars", tableName).
//        where("load_date = ?", date).
//        select("load_date").
//        limit(1).
//        collect
//    }.toList
//
//    val indexWithLatestLoadDate = datesWithRecords.indexWhere(_.nonEmpty)
//
//    if (indexWithLatestLoadDate == -1) {
//      Utility.Constants.ETLFirstLoadDate
//    } else (currentDay.minusDays(datesWithRecords.indexWhere(_.nonEmpty)))
//
//  }
//
//  def createPropCar(acqCarHeader:AcqCarHeader):PropCar = {
//    //val acqCarHeader = AcqCarHeader("Volkswagen Passat 1,6 TDI 105hk BlueMotion Business","http://m.finn.no/car/used/ad.html?finnkode=78537231","Kirkenes","2010","121 835 km","149 000,-",2016-07-04 13:41:21.477,2016-07-04))
//
//    val prevAcqCarDetails = _csc.sparkContext.cassandraTable[AcqCarDetails]("finncars", "acq_car_details").
//      where("url = ?", acqCarHeader.url).
//      where("load_time <= ?", new java.util.Date(acqCarHeader.load_time)).
//      filter(row => row.deleted == false).collect
//
//    if (prevAcqCarDetails.length > 0) {
//      val acqCarDetails = prevAcqCarDetails.maxBy(_.load_time)
//      val propertiesMap:HashMap[String,String] = Utility.getMapFromJsonMap(acqCarDetails.properties)
//      val equipmentList:Set[String] = Utility.getSetFromJsonArray(acqCarDetails.equipment)
//      PropCar(url=acqCarHeader.url,
//        finnkode=Utility.parseFinnkode(acqCarHeader.url),
//        location=acqCarHeader.location,
//        title=acqCarHeader.title,
//        year=Utility.parseYear(acqCarHeader.year),
//        km=Utility.parseKM(acqCarHeader.km),
//        price=getLastPrice(acqCarHeader.price, acqCarHeader.url, acqCarHeader.load_date, acqCarHeader.load_time),
//        properties=propertiesMap,
//        equipment=equipmentList,
//        information=acqCarDetails.information,
//        sold=Utility.carMarkedAsSold(acqCarHeader.price),
//        deleted=acqCarDetails.deleted,
//        load_time=acqCarHeader.load_time,
//        load_date=acqCarHeader.load_date)
//    } else {
//      PropCar(url=acqCarHeader.url,
//        finnkode=Utility.parseFinnkode(acqCarHeader.url),
//        location=acqCarHeader.location,
//        title=acqCarHeader.title,
//        year=Utility.parseYear(acqCarHeader.year),
//        km=Utility.parseKM(acqCarHeader.km),
//        price=getLastPrice(acqCarHeader.price, acqCarHeader.url, acqCarHeader.load_date, acqCarHeader.load_time),
//        properties=Utility.Constants.EmptyMap,
//        equipment=Utility.Constants.EmptyList,
//        information=Utility.Constants.EmptyString,
//        sold=Utility.carMarkedAsSold(acqCarHeader.price),
//        deleted=true,
//        load_time=acqCarHeader.load_time,
//        load_date=acqCarHeader.load_date)
//    }
//
//
//  }
//
//}