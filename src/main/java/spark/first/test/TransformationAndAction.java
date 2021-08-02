/**
 * 
 */
package spark.first.test;

import java.util.Arrays;
import java.util.List;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author sairaj
 *
 */
public class TransformationAndAction {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*Transformation*/
		
		JavaSparkContext sc=new Spark().sparkconf();
		
		List<Integer>intlist=Arrays.asList(1,2,4,5,6,7,8,9);
		List<String> stringlist=Arrays.asList("Databricks would like to give a special thanks to Jeff Thomspon for contributing 67 visual diagrams depicting the Spark API under the MIT license to the Spark community");
		
		JavaRDD<Integer> intrdd= sc.parallelize(intlist);
		JavaRDD<String> stringrdd= sc.parallelize(stringlist);
		
		/*map*/
		
		JavaRDD<Integer> addnum=intrdd.map( f -> f+1);
		//JavaRDD<Integer> pairrdd= intrdd.map(f -> (f , 1 ));
		JavaRDD<Integer> stringexp= stringrdd.map(x -> x.split(" ").length);
		
			/*action*/
//		addnum.collect();
//		stringexp.collect();
		
		addnum.foreach(x -> System.out.println(x));
		stringexp.foreach(y->System.out.println(y));

	}

}
