package com.ibm.dataFtiltering.jfc2PlantDataFilterEngine.test

 import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession
import com.mongodb.spark.sql._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame,SQLContext, Row}
//import org.apache.spark.sql.types._


import org.apache.spark.SparkConf

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.scala.Logging
import org.apache.logging.log4j.Level    

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import org.apache.kafka.clients.producer._

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties;
import org.apache.kafka.common.serialization._
import org.apache.spark.broadcast._
import org.apache.spark.sql.types.{StringType, FloatType,TimestampType,StructField, StructType}



object TestPlantDataFilterEngine {
  
   
 
  
  def main (args: Array[String]): Unit = {
    
    val logger = LoggerFactory.getLogger("Plant Data Filter")
    val config = ConfigFactory.parseResources("application.conf")
    val conf = new SparkConf().setMaster("local[*]").setAppName("PlantDataFilterEngine") 
    
    val sc = new StreamingContext(conf, Seconds(config.getLong("window.timeStamp")))
    val kafkaConf = Map( "metadata.broker.list" -> config.getString("KAFKA_BROKERS"), "zookeeper.connect" -> config.getString("zookeeper.conect"), "group.id" -> "condition monitoring", "zookeeper.connection.timeout.ms" -> "1000")
 // topic names which will be read
    val topics = Array(config.getString("kafka.TOPIC"))

    // create kafka direct stream object
    val stream = KafkaUtils.createDirectStream[String, String](sc, PreferConsistent, Subscribe[String, String](topics, kafkaConf))
      stream.foreachRDD { rdd =>
        print(rdd.toString())
      }
   }
  
}