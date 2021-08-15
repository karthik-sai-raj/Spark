/**
 * 
 */
package spark.first.test;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author sairaj
 * 
 * 
 * 
	python
	 
	 
	  
	  rdd=sc.textFile("src/main/resources/wordcount.txt",3)
	  rdd2=rdd.flatMap(lambda x: x.split(" "))
	  rdd3=rdd2.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)
	 
	 
 *
 */

public class wordCountProject {

	/**
	 * @param args
	 */

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JavaSparkContext sc= new Spark().sparkconf();
		
		/*Read a text file to rdd*/
		
		JavaRDD<String> lines=sc.textFile("src/main/resources/wordcount.txt",3);//3 partition's to file 
		//lines.foreach(x-> System.out.println(x));
		/*split into words*/
		/*JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String,String>() {

			@Override
			public Iterator<String> call(String t) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(t.split(" ")).iterator();
			}
					
				});
		*/
		/*or */
		JavaRDD<String> words=lines.flatMap(t -> Arrays.asList(t.split(" ")).iterator());
		//words.foreach(x-> System.out.println(x));
		JavaPairRDD<String, Integer>pair=words.mapToPair(new PairFunction<String, String, Integer>(){

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				String x;
				/*
				 * to remove , and . from from rdd words*/
				x=t.contains(",") ? (t.replace(",", "")): t.contains(".")? (t.replace(".", "")):t;
				
				
				return new Tuple2<String, Integer>(x,1);
			}
			
		}).reduceByKey(new Function2<Integer,Integer,Integer>(){

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
			
		});
		pair.foreach(f-> System.out.println(f));

	}

}

