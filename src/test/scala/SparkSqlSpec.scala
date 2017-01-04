package ttorbjornsen.finncars

import org.scalatest.Suite
import org.apache.spark.sql.SQLContext

trait SparkSqlSpec extends SparkSpec {
  this: Suite =>

  private var _sqlCtx = new SQLContext(sc)


  def sqlCtx: SQLContext = _sqlCtx

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sqlCtx = new SQLContext(sc)
  }

  override def afterAll(): Unit = {
    _sqlCtx = null

    super.afterAll()
  }

}