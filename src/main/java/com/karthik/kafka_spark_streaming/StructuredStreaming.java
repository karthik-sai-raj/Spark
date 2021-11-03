/**
 * 
 */
package com.karthik.kafka_spark_streaming;

import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import com.karthik.conf.SparkConfig;

/**
 * @author sairaj
 *
 */
public class StructuredStreaming {

	/**
	 * @param args
	 * @throws TimeoutException 
	 * @throws StreamingQueryException 
	 */
	public static void main(String[] args) throws TimeoutException, StreamingQueryException {
		
		SparkSession ss=new SparkConfig().getSession();
		
		Dataset<Row> df=ss.readStream().format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "viewrecords").option("includeHeaders", true).option("group.id","executor_group").load();
		if(df != null) {
			System.out.println("working");
		}
		else
			System.out.println("nope");
		
		//starting df operations
		df.createOrReplaceTempView("sqltable");
		Dataset<Row> result = ss.sql("select cast(value as STRING) from sqltable");
		
		
		StreamingQuery query = result.writeStream().format("console").outputMode(OutputMode.Append()).start();
		
		query.awaitTermination();
	}

}
