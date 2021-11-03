package com.karthik.kafka_spark_streaming;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


import com.karthik.conf.Kafkaserverdetails;

import com.karthik.conf.KafkaProperties;


public class KakaMessageProducer {
	
	public static void main(String args[]) {
	
	String bootstrapserver="";
	//creationg kafka properties
	Properties prop = new KafkaProperties().getProp() ;
	//kafka producer
	KafkaProducer<Integer,String>producer= new KafkaProducer<>(prop);
	
	String csvpath="src/main/resources/students.csv";
        
	
	try {
		Stream<String> FileStream = Files.lines(Paths.get(csvpath));
		System.out.println("sendind data");
		FileStream.forEach(line -> {
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(Kafkaserverdetails.topic, line);
          //Kafka producer record
            producer.send(record);
            
            try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        );
		System.out.println("completed");
	}
	catch(IOException e) {
		 e.printStackTrace();
	}
	

         
         
         
  }
}

