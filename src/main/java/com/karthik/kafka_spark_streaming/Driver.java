/**
 * 
 */
package com.karthik.kafka_spark_streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.karthik.conf.SparkConfig;

/**
 * @author sairaj
 *
 */
public class Driver {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SparkSession ss=new SparkConfig().getSession();
		
		Dataset<Row> csv = ss.read().option("header", true).csv("src/main/resources/students.csv");
		
		csv.show();
		
		ss.close();
		
	}

}
