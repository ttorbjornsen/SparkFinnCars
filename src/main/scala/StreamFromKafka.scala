package ttorbjornsen.finncars
import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
/**
  * Created by torbjorn on 29.12.16.
  */
object StreamFromKafka extends App {

  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlCtx = new SQLContext(sc)


  val kafkaParams = Map("metadata.broker.list" -> "kafka:9092")//, "auto.offset.reset" -> "smallest")
  val topics = Set("acq_car_header")
  val ssc = new StreamingContext(sc, Seconds(5)) //60 in production
  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  directKafkaStream.foreachRDD(
    (rdd, time) => {
      if (rdd.toLocalIterator.nonEmpty) {
        //when new data from Kafka is available
        println(rdd.count + " new Kafka messages to process")
        val content = rdd.map(_._2)

        content.foreach { jsonDoc =>
          val jsonCarHdr: JsValue = Json.parse(jsonDoc.mkString)
          val acqCarHdrList = Utility.createAcqCarHeaderObjects(jsonCarHdr)
          val carHdrDF = sqlCtx.createDataFrame(acqCarHdrList)

          carHdrDF.write.
          format("org.apache.spark.sql.cassandra").
          options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
          mode(SaveMode.Append).
          save

          println(acqCarHdrList.length.toString + " records written to Cassandra table acq_car_h")

          val lastSuccessfulLoadHDf = sqlCtx.createDataFrame(Seq(LastSuccessfulLoad("acq_car_h",System.currentTimeMillis())))
          lastSuccessfulLoadHDf.write.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "last_successful_load", "keyspace" -> "finncars")).
            mode(SaveMode.Append).
            save


          val acqCarDetailsList = acqCarHdrList.map { carHeader =>
            Utility.createAcqCarDetailsObject(carHeader)
          }

          val carDetDF = sqlCtx.createDataFrame(acqCarDetailsList)
          carDetDF.write.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "acq_car_d", "keyspace" -> "finncars")).
            mode(SaveMode.Append).
            save

          println(acqCarDetailsList.length.toString + " records written to Cassandra table acq_car_d")

          carHdrDF.select("finnkode", "load_time", "load_date").write.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "scraping_log", "keyspace" -> "finncars")).
            mode(SaveMode.Append).
            save

          println(acqCarHdrList.length.toString + " log records written to Cassandra table scraping_log")

          val lastSuccessfulLoadDDf = sqlCtx.createDataFrame(Seq(LastSuccessfulLoad("acq_car_d",System.currentTimeMillis())))
          lastSuccessfulLoadDDf.write.
            format("org.apache.spark.sql.cassandra").
            options(Map("table" -> "last_successful_load", "keyspace" -> "finncars")).
            mode(SaveMode.Append).
            save

        }
      }})



  ssc.start()
  //ssc.stop(false) //for debugging in REPL
  ssc.awaitTermination()
}


