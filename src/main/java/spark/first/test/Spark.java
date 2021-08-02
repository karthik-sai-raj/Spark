/**
 * 
 */
package spark.first.test;

import java.util.Arrays;
import java.util.List;


import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author saira
 *
 */
public class Spark {
	public JavaSparkContext sparkconf() {
		final Logger log= LoggerFactory.getLogger(Spark.class);
	/*spark configuration*/
	SparkConf sc= new SparkConf()
			.setAppName("Karthik-Spark-Application")
			.setMaster("local[*]")
			/*.set("spark.ui.port","8080")*/
			.set("spark.exector.memory","2g");
	log.info(" returning SparkConf ");
	JavaSparkContext jsc = new JavaSparkContext(sc);
	return jsc;
	}
	
	/*spark context*/
//	 JavaSparkContext jsc = new JavaSparkContext(sc);
//	List<String> list=Arrays.asList("Karthik","is","learning","Apache","spark");
	/*2 partitions*/
//	JavaRDD<String> rdd= jsc.parallelize(list,2);
//	 sc.parallelize(Arrays.asList("pandas", "i like pandas"));
	 
//	JavaPairRDD<String,Integer> rdd2=rdd.map(f => (f,f.length));
//	rdd.foreach(x->{
//		log.info("x "+ x);
//	});
	 
	 
}

