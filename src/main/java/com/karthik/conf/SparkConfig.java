/**
 * 
 */
package com.karthik.conf;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

/**
 * @author saira
 *
 */
public class SparkConfig {
	
	public SparkConf getSpark() {
		SparkConf sc=new SparkConf().setAppName("kafka_spark_streaming").setMaster("local[*]");
		
		return sc;
		
	}
	public SparkSession getSession() {
		SparkSession ss=  SparkSession.builder()
										.appName("kafka_spark_streaming")
										.master("local[*]")
										.config("spark.sql.warehouse.dir", "file:///D:/work-space/spark.first.test/")
										.getOrCreate();
		return ss;
		
	}

}
