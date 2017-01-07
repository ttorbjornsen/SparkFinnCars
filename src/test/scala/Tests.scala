package ttorbjornsen.finncars

import java.time.LocalDate
import java.util.HashMap
import ttorbjornsen.finncars.Utility

import scala.collection.immutable.Map
import java.time.{Instant, LocalDate, LocalTime}
import java.util.HashMap
import scala.concurrent.duration._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql._
import com.datastax.spark.connector._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

import org.apache.spark.rdd.RDD
import org.scalatest.{FunSpec, Matchers}





/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers with SparkSqlSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()

//    val dfRandomCarHeader = _csc.read.
//      format("org.apache.spark.sql.cassandra").
//      options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
//      load().
//      select("title", "url", "location", "year", "km", "price", "load_time", "load_date").
//      limit(1)
    //REPL : USE val
  }


  describe("application") {
    it("should be able to generate correct url from finnkode") {
      val finnkode = 88450076
      val finnkodeUrl = Utility.generateFinnCarUrl(finnkode)
      finnkodeUrl should equal ("http://m.finn.no/car/used/ad.html?finnkode=88450076")
    }

    it("should be able to create a list of AcqCarHeader objects from a list of AcqCarHeader objects"){
      val acqCarHeaderList = List(AcqCarHeader(finnkode=88450076, url=Utility.generateFinnCarUrl(88450076)), AcqCarHeader(finnkode=87252432, url=Utility.generateFinnCarUrl(87252432)))
      val acqCarDetailsList = acqCarHeaderList.map { carHeader =>
        Utility.createAcqCarDetailsObject(carHeader)
      }
      acqCarDetailsList(0).properties contains("131 Hk") //preferably write better test
      acqCarDetailsList(1).url
    }

    it("temp - dataset conversion"){
      //TEMP TO TRY OUT DATASET
      val spark = SparkSession.
        builder().
        appName("Spark SQL basic example").
        master("local[*]").
        config("spark.some.config.option", "some-value").
        getOrCreate()
      import spark.implicits._

      object Utility {
        object Constants {
        val EmptyMap = new java.util.HashMap[String,String](Map("NULL" -> "NULL"))
        val EmptyList = Set("NULL")
        val EmptyString = "NULL"
        val EmptyInt = -1
        val EmptyDate = "1900-01-01"
        val EmptyUtilDate = new java.sql.Date(1900,1,1)
        val ETLSafetyMargin = 7 //days
        val ETLFirstLoadDate = LocalDate.of(2016,7,1)
      }
    }
      case class AcqCarDetails(finnkode:Int= Utility.Constants.EmptyInt, load_date:java.sql.Date= Utility.Constants.EmptyUtilDate, load_time:Long= Utility.Constants.EmptyInt, properties:String= Utility.Constants.EmptyString, equipment:String= Utility.Constants.EmptyString, information:String= Utility.Constants.EmptyString, deleted:Boolean=false, url:String=Utility.Constants.EmptyString)
      case class PropCar(finnkode:Int = Utility.Constants.EmptyInt, load_date:java.sql.Date= Utility.Constants.EmptyUtilDate, title:String= Utility.Constants.EmptyString, location:String= Utility.Constants.EmptyString, year: Int= Utility.Constants.EmptyInt, km: Int= Utility.Constants.EmptyInt, price: Int= Utility.Constants.EmptyInt, properties:java.util.HashMap[String,String]= Utility.Constants.EmptyMap, equipment:Set[String]= Utility.Constants.EmptyList, information:String= Utility.Constants.EmptyString, sold:Boolean=false, deleted:Boolean=false, load_time:Long = Utility.Constants.EmptyInt, url:String=Utility.Constants.EmptyString)

      val seq = Seq(AcqCarDetails(properties = "{temp:2}))"))
      implicit val propCarEncoder = org.apache.spark.sql.Encoders.kryo[PropCar]
      val seq2 = Seq(PropCar(properties = new java.util.HashMap(Map("test" -> "test"))))
      val seq3 = sqlCtx.createDataFrame(seq2)
      val d = spark.createDataset(seq2)
      d.first.properties


    }


  }
}