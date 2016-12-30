import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import play.api.libs.json._
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.SparkSession

/**
  * Created by torbjorn on 29.12.16.
  */
object StreamFromKafka extends App {

  val conf = new SparkConf().setAppName("loadRaw").setMaster("local[*]").set("spark.cassandra.connection.host","finncars-cassandra")
  //val conf = new SparkConf().setAppName("loadRaw").setMaster("spark://torbjorn-VirtualBox:7077").set("spark.cassandra.connection.host","192.168.56.56")

  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val kafkaParams = Map("metadata.broker.list" -> "kafka:9092")//, "auto.offset.reset" -> "smallest")
  val topics = Set("acq_car_header")
  //val fromOffsets = Map(new TopicAndPartition("finnCars", 0) -> 0L)
  //val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, fromOffsets, (mmd:MessageAndMetadata[String, String]) => mmd)
  val ssc = new StreamingContext(sc, Seconds(5)) //60 in production
  val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
  directKafkaStream.foreachRDD(
    (rdd, time) => {
      if (rdd.toLocalIterator.nonEmpty) {
        //when new data from Kafka is available
        println(rdd.count + " new Kafka messages to process")
        //val content = rdd.map(_._2).collect

        }
      })

  ssc.start()
  //ssc.stop(false) //for debugging in REPL
  ssc.awaitTermination()
}


