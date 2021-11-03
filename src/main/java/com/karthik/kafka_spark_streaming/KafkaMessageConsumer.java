package com.karthik.kafka_spark_streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import  org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.karthik.conf.Kafkaserverdetails;
import com.karthik.conf.SparkConfig;

import scala.Tuple2;


public class KafkaMessageConsumer {
	public static void main(String args[]) throws StreamingQueryException, TimeoutException, InterruptedException {
		
        //Creating consumer properties  
        Properties properties=new Properties();  
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafkaserverdetails.bootstrapserver);  
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   IntegerDeserializer.class.getName());  
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());  
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"executor_group");  
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");  
        //creating consumer  spark-kafka-source-d58eb85e-679a-4803-8a7b-51f397cbb123-858970771-driver-0
        KafkaConsumer<Integer,String> consumer= new KafkaConsumer<Integer,String>(properties);  
        //Subscribing  
                consumer.subscribe(Arrays.asList(Kafkaserverdetails.topic)); 
                


                while (true) {
                    ConsumerRecords<Integer, String> records=consumer.poll(10000);
                    for (ConsumerRecord<Integer, String> record : records)
                    
                    // print the offset,key and value for the consumer records.
                    System.out.printf("offset = %d, key = %s, value = %s\n", 
                       record.offset(), record.key(), record.value());
                 }       
		
		
		}
		
	}



