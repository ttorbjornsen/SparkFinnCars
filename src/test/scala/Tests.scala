package ttorbjornsen.finncars
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


  }
}