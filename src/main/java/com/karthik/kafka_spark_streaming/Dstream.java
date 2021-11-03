/**
 * 
 */
package com.karthik.kafka_spark_streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.karthik.conf.SparkConfig;

/**
 * @author saira
 *
 */
public class Dstream {

	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		JavaStreamingContext conf= new  JavaStreamingContext(new SparkConfig().getSpark(), new Duration(5000));

	
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", IntegerDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "executor_group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		Collection<String> topics = Arrays.asList("viewrecords");
		System.out.println("hi");
		JavaInputDStream<ConsumerRecord<Integer, String>> stream =
		  KafkaUtils.createDirectStream(
		    conf,
		    LocationStrategies.PreferConsistent(),
		    ConsumerStrategies.Subscribe(topics, kafkaParams)
		  );
		
		System.out.println("hi");
		JavaDStream<Long> l=stream.count();
		
		if(stream!=null) {
			System.out.println("hi 3");
			stream.foreachRDD(rdd->{
				System.out.println("hi foreach rdd");
				rdd.foreachPartition(f-> {
					String str="";
					System.out.println("hi for each partition");
					while(f.hasNext()) {
						ConsumerRecord<Integer, String> consumer = f.next();
						String value = consumer.value();
						System.out.println(value);
					}//while
				});//partition
			});
					
					conf.start();
					conf.awaitTermination();

	}

}
}