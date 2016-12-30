FROM gettyimages/spark
ADD ./SparkFinnCars-1.0-SNAPSHOT.jar /usr/src/sparkfinncars/
#WORKDIR /usr/src/sparkfinncars
ENTRYPOINT ["spark-submit","--class", "StreamFromKafka", "/usr/src/sparkfinncars/SparkFinnCars-1.0-SNAPSHOT.jar"]
#CMD ["--class", "StreamFromKafka", "/usr/src/sparkfinncars/SparkFinnCars-1.0-SNAPSHOT.jar"]
#CMD ["--class StreamFromKafka", "/usr/src/sparkfinncars/SparkFinnCars-1.0-SNAPSHOT.jar"]
#CMD java -jar SparkFinnCars-1.0-SNAPSHOT.jar
#CMD ping 192.168.56.56