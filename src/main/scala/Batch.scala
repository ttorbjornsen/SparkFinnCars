package ttorbjornsen.finncars
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by torbjorn on 04.01.17.
  */
object Batch {
  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlCtx = new SQLContext(sc)


}
