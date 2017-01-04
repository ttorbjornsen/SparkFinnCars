import org.apache.spark.rdd.RDD
import org.mkuthan.spark.SparkSqlSpec
import org.scalatest.{FunSpec, Matchers}



/**
  * Created by torbjorn.torbjornsen on 06.07.2016.
  */
class Tests extends FunSpec with Matchers with SparkSqlSpec {

  override def beforeAll(): Unit = {
    super.beforeAll()

    val dfRandomCarHeader = _csc.read.
      format("org.apache.spark.sql.cassandra").
      options(Map("table" -> "acq_car_header", "keyspace" -> "finncars")).
      load().
      select("title", "url", "location", "year", "km", "price", "load_time", "load_date").
      limit(1)
    //REPL : USE val
  }

  describe("application") {
    it("should be able to extract and correctly parse details page") {

    }


  }
}