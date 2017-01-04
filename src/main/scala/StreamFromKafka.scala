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
//          println("JSONDOC")
//          println(jsonDoc)
          val jsonCarHdr: JsValue = Json.parse(jsonDoc.mkString)
          val acqCarHdrList = Utility.createAcqCarHeaderObjects(jsonCarHdr)
          val carHdrDF = sqlCtx.createDataFrame(acqCarHdrList)

          carHdrDF.write.
          format("org.apache.spark.sql.cassandra").
          options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
          mode(SaveMode.Append).
          save

          //val acqCarHdrList = List(
          val acqCarHdrUrlList = acqCarHdrList.map(_.finnkode)

          for (acqCarHdr <- acqCarHdrList){

            }

//          sqlCtx.read.
//            format("org.apache.spark.sql.cassandra").
//            options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
//            load().show

        }
      }})



  ssc.start()
  //ssc.stop(false) //for debugging in REPL
  ssc.awaitTermination()
}


