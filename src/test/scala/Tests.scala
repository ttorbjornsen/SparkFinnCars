package ttorbjornsen.finncars

import org.apache.spark
import org.apache.spark.SparkConf

import scala.collection.immutable.Map
import java.time.{Instant, LocalDate, LocalTime}

import scala.collection.JavaConversions._
import org.apache.spark.sql._

import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

//import ttorbjornsen.finncars.Utility //import this in REPL, otherwise not necessary since in same package
import org.scalatest.{FunSpec, Matchers}
import org.apache.spark.{SparkContext, SparkConf}
//import ttorbjornsen.finncars.PropCar //import this in REPL, otherwise not necessary since in same package
//import ttorbjornsen.finncars.BtlCar //import this in REPL, otherwise not necessary since in same package



/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers with SparkSqlSpec {

  val cassandraDockerIp = "172.20.0.5"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]")//.set("spark.cassandra.connection.host","finncars-cassandra")
    val spark = SparkSession.
      builder().
      config(conf).
      appName("BatchApp").
//      master("local[*]").
//      config("spark.cassandra.connection.host", "finncars-cassandra").
      getOrCreate()

    import spark.implicits._ //e.g. convert from data frame to dataset


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

    it("must create propcar from acqCarHeader and acqCarDetails"){

//      case class AcqCarHeader(finnkode:Int= Utility.Constants.EmptyInt, load_date:Long = Utility.Constants.EmptyInt, load_time:Long= Utility.Constants.EmptyInt, title:String= Utility.Constants.EmptyString, location:String= Utility.Constants.EmptyString, year: String= Utility.Constants.EmptyString, km: String= Utility.Constants.EmptyString, price: String= Utility.Constants.EmptyString, url:String=Utility.Constants.EmptyString)
//      val acqCarH = spark.createDataset(Seq(
//        AcqCarHeader(24176155,1483488000,1483566705,"Audi A6 2.4 Auto/Tiptronic+cruisecontrol","Kolbotn","2003","130 000 km","99 500,-","http://m.finn.no/car/used/ad.html?finnkode=24176155"),
//        AcqCarHeader(24176155,1483401600,1483401600,"Audi A6 2.4 Auto/Tiptronic+cruisecontrol","Kolbotn","2003","130 000 km","99 500,-","http://m.finn.no/car/used/ad.html?finnkode=24176155"),
//        AcqCarHeader(24176155,1483315200,1483315200,"Audi A6 2.4 Auto/Tiptronic+cruisecontrol","Kolbotn","2003","130 000 km","99 500,-","http://m.finn.no/car/used/ad.html?finnkode=24176155")
//      ))
//
//      val acqCarD= spark.createDataset(Seq(
//        AcqCarDetails(24176155,1483488000,1483566720,""""{"Salgsform":"Bruktbil til salgs","Antall seter":"5","Girkasse":"Automat","Effekt":"170 Hk","Drivstoff":"Bensin","Antall dører":"5","Karosseri":"Stasjonsvogn","Antall eiere":"2","Hjuldrift":"Forhjulsdrift","Bilen står i":"Norge","Avgiftsklasse":"Personbil","Fargebeskrivelse":"brillient sølvmett","Km.stand":"130 000 km","1. gang registrert":"05.09.2002","Interiørfarge":"Grå skinn","Årsmodell":"2003","Farge":"Sølv","Sylindervolum":"2,4 l","CO2 utslipp":"235 g/km"}""",""""["Lettmet. felg sommer","Lettmet. felg vinter","Metallic lakk","Parkeringsensor bak","Sommerhjul","Takrails","Vinterhjul","Xenonlys","Bagasjeromstrekk","Cruisekontroll","Elektrisk sete u. memory","Elektriske speil","El.vinduer","Farget glass","Kjørecomputer","Klimaanlegg","Midtarmlene","Multifunksjonsratt","Oppvarmede seter","Radio/CD","Sentrallås","Servostyring","Skinninteriør","Sportsseter","Takluke","ABS-bremser","Airbag foran","Antiskrens","Antispinn","Sideairbager","Startsperre"]""","Info about my car",false,"http://m.finn.no/car/used/ad.html?finnkode=24176155"),
//        AcqCarDetails(24176155,1483401600,1483401600,""""{"Salgsform":"Bruktbil til salgs","Antall seter":"5","Girkasse":"Automat","Effekt":"170 Hk","Drivstoff":"Bensin","Antall dører":"5","Karosseri":"Stasjonsvogn","Antall eiere":"2","Hjuldrift":"Forhjulsdrift","Bilen står i":"Norge","Avgiftsklasse":"Personbil","Fargebeskrivelse":"brillient sølvmett","Km.stand":"130 000 km","1. gang registrert":"05.09.2002","Interiørfarge":"Grå skinn","Årsmodell":"2003","Farge":"Sølv","Sylindervolum":"2,4 l","CO2 utslipp":"235 g/km"}""",""""["Lettmet. felg sommer","Lettmet. felg vinter","Metallic lakk","Parkeringsensor bak","Sommerhjul","Takrails","Vinterhjul","Xenonlys","Bagasjeromstrekk","Cruisekontroll","Elektrisk sete u. memory","Elektriske speil","El.vinduer","Farget glass","Kjørecomputer","Klimaanlegg","Midtarmlene","Multifunksjonsratt","Oppvarmede seter","Radio/CD","Sentrallås","Servostyring","Skinninteriør","Sportsseter","Takluke","ABS-bremser","Airbag foran","Antiskrens","Antispinn","Sideairbager","Startsperre"]""","Info about my car",false,"http://m.finn.no/car/used/ad.html?finnkode=24176155"),
//        AcqCarDetails(24176155,1483315200,1483315200,""""{"Salgsform":"Bruktbil til salgs","Antall seter":"5","Girkasse":"Automat","Effekt":"170 Hk","Drivstoff":"Bensin","Antall dører":"5","Karosseri":"Stasjonsvogn","Antall eiere":"2","Hjuldrift":"Forhjulsdrift","Bilen står i":"Norge","Avgiftsklasse":"Personbil","Fargebeskrivelse":"brillient sølvmett","Km.stand":"130 000 km","1. gang registrert":"05.09.2002","Interiørfarge":"Grå skinn","Årsmodell":"2003","Farge":"Sølv","Sylindervolum":"2,4 l","CO2 utslipp":"235 g/km"}""",""""["Lettmet. felg sommer","Lettmet. felg vinter","Metallic lakk","Parkeringsensor bak","Sommerhjul","Takrails","Vinterhjul","Xenonlys","Bagasjeromstrekk","Cruisekontroll","Elektrisk sete u. memory","Elektriske speil","El.vinduer","Farget glass","Kjørecomputer","Klimaanlegg","Midtarmlene","Multifunksjonsratt","Oppvarmede seter","Radio/CD","Sentrallås","Servostyring","Skinninteriør","Sportsseter","Takluke","ABS-bremser","Airbag foran","Antiskrens","Antispinn","Sideairbager","Startsperre"]""","Info about my car",false,"http://m.finn.no/car/used/ad.html?finnkode=24176155")
//      ))
//      val propCarSeq = Range(0,2).map(i => Utility.createPropCar(acqCarH, acqCarD))
//      implicit val propCarEncoder = org.apache.spark.sql.Encoders.kryo[PropCar]
//
//      val propCarDF = spark.createDataset(propCarSeq)
//      import spark.implicits._
//      propCarDF.map(temp => println(temp))

    }
    it("must create btlcar from propcar"){
      val finnkode = 89690927

      val spark = SparkSession.
        builder().
        appName("BatchApp").
        master("local[*]").
        config("spark.cassandra.connection.host", cassandraDockerIp).
        getOrCreate()

      import spark.implicits._ //e.g. convert from data frame to dataset

      val propCarFinnkodeArray = spark.read.
        format("org.apache.spark.sql.cassandra").
        options(Map("table" -> "prop_car_daily", "keyspace" -> "finncars")).
        load.
        filter("finnkode = " + finnkode).
        orderBy("load_date").
        as[PropCar].
        collect

      propCarFinnkodeArray.map(car => println(car.load_date))

      val btlCar1 = Utility.createBtlCar(propCarFinnkodeArray)
      btlCar1.sold should equal(true)
      btlCar1.lead_time_sold should equal(2)


//      val buf = scala.collection.mutable.ListBuffer.empty[BtlCar]
//      buf += btlCar1
//
//      val tempDS = buf.filter(_ != BtlCar()).toSeq.toDS()
//      tempDS.write.
//        format("org.apache.spark.sql.cassandra").
//        options(Map("table" -> "btl_car", "keyspace" -> "finncars")).
//        mode(SaveMode.Append).save()
//
//      val temp = spark.read.
//        format("org.apache.spark.sql.cassandra").
//        options(Map("table" -> "btl_car", "keyspace" -> "finncars")).
//        load.
//        filter("finnkode = " + finnkode).
//        as[BtlCar].
//        collect
//
//      temp(0).last_updated


      //propCarsDF.show

    }







  }
}