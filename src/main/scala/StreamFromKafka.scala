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
          println("JSONDOC")
          println(jsonDoc)
          val jsonCarHdr: JsValue = Json.parse(jsonDoc.mkString)
          val acqCarHdr = Utility.createAcqCarHeaderObjects(jsonCarHdr)
          val carHdrDF = sqlCtx.createDataFrame(acqCarHdr)

          carHdrDF.write.
          format("org.apache.spark.sql.cassandra").
          options(Map("table" -> "acq_car_h", "keyspace" -> "finncars")).
          save

          println("JSONCARHDR")

//          val headerUrl = jsonCarHdr.\\("url").head.as[String]
//          val numOfCars = jsonCarHdr.\\("group")(0).as[JsArray].value.size
//          val acqCarHeaderList = Range(0, numOfCars).map(i =>
//            Utility.createAcqCarHeaderObject(i, jsonCarHdr)).toList
//        }
//
//        val df = sqlCtx
//          .read
//          .format("org.apache.spark.sql.cassandra")
//          .options(Map( "table" -> "keyspaces", "keyspace" -> "system_schema" ))
//          .load()
//
//        df.show()


        //val content = rdd.map(_._2).collect

        }
      }})

  ssc.start()
  //ssc.stop(false) //for debugging in REPL
  ssc.awaitTermination()
}


