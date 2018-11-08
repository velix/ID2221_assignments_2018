package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    // make a connection to Kafka and read (key, value) pairs from it
    // Configurations
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000")
    val conf = new SparkConf().setAppName("Simple Streaming Application").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaTopics = Set("avg")
    ssc.checkpoint("checkpoint")

    // Set the stream
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc, kafkaConf, kafkaTopics)

    // This is where the actually processesing starts:
    
    // Map the value part received into a new K V map.
    val pairs = messages.map(x => {
      val s = x._2.split(",")
      (s(0),s(1).toDouble)
    })

    // measure the sum value for each key in a stateful manner
    def sumFunc(key: String, value: Option[Double], state: (State[Double])): (String, Double) = {
      // Make sure there's an initial state
      if(state.exists()){
        // Guard against faulty data with getOrElse (probably doesn't happen)
        val newSum = state.get+value.get
        state.update(newSum)
        return (key, newSum)
      } else{
        state.update(value.getOrElse(0))
        return (key, value.getOrElse(0))
      }
    }
    // measure the sum value for each key in a stateful manner
    def countFunc(key: String, value: Option[Double], state: (State[Double])): (String, Double) = {
      // Make sure there's an initial state
      if(state.exists()){
        // Guard against faulty data with getOrElse (probably doesn't happen)
        val newCount = state.get+1
        state.update(newCount)
        return (key, newCount)
      } else{
        state.update(1)
        return (key, 1)
      }
    }

    // Apply the mapping function
    val sumDstream = pairs.mapWithState(StateSpec.function(sumFunc _))
    val countDstream = pairs.mapWithState(StateSpec.function(countFunc _))
    val joinedDstream = sumDstream.join(countDstream)
    val averageDstream = joinedDstream.map{case (key, (sum,count)) => {
      println(key+": "+sum+" / "+count)
      (key, sum/count)
    }}
    // store the result in Cassandra
    averageDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))
    // Also print something useful..
    averageDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
