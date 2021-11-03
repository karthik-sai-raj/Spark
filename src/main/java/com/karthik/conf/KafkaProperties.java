package com.karthik.conf;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProperties {
	
	public Properties getProp() {
		
		
		Properties prop=new Properties();
		prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Kafkaserverdetails.bootstrapserver);
		prop.setProperty(ProducerConfig.CLIENT_ID_CONFIG, Kafkaserverdetails.clientID );
		prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
		prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return prop;
		
	}

}
