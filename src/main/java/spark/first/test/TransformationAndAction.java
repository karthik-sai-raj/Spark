/**
 * 
 */
package spark.first.test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author sairaj
 *
 */
public class TransformationAndAction {

	/**
	 * @param args
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		/*Transformation*/
		
		JavaSparkContext sc=new Spark().sparkconf();
		
		List<Integer>intlist=Arrays.asList(1,2,4,5,5,6,7,8,9);
		List<String> stringlist=Arrays.asList("Databricks would like to give a special thanks to Jeff Thomspon for contributing 67 visual diagrams depicting the Spark API under the MIT license to the Spark community");
		
		JavaRDD<Integer> intrdd= sc.parallelize(intlist);
		JavaRDD<String> stringrdd= sc.parallelize(stringlist);
	
		/*map*/
		System.out.println("MAP");
		JavaRDD<Integer> addnum=intrdd.map( f -> f+1);
		

		JavaRDD<String> stringexp= stringrdd.map(x -> x.split(" ").toString());
		
			/*action*/
//		addnum.collect();
//		stringexp.collect();
		System.out.println("MAPtopair + pairrdd");
		addnum.foreach(x -> System.out.println(x));
		//System.out.println("MAP +y");
		//stringexp.foreach(y->System.out.println(y));
		
		
		/*FLAT-MAP*/
		System.out.println("FLAT-MAP +rdd1");
		JavaRDD<Integer> fmrdd1=intrdd.flatMap(f ->{
			
			int x=f+1;
			int mul=f*3;
			int r=f;
			return  Arrays.asList(x,mul,r).iterator();
			
		});
		//fmrdd1.collect();
		fmrdd1.foreach(y->System.out.println(y));
		System.out.println("FLAT-MAP +rdd2");
		JavaRDD<String> fmrdd2=stringrdd.flatMap(s-> Arrays.asList(s.split(" ")).iterator());
		//fmrdd2.collect();
		fmrdd2.foreach(y->System.out.println(y));
		
		
		
		/*FILTER*/
		
		JavaRDD<Integer> GreaterThatFive =sc.parallelize(Arrays.asList(5,58,2,65,6,4));
		
		JavaRDD<Integer> result =GreaterThatFive.filter(x->(x>5));
		
		JavaRDD<String> filterString=fmrdd2.filter(x->   x.startsWith("d")|| x.startsWith("A"));//using flat map here fmrdd2
		
		result.foreach(x->System.out.println(x));
		
		filterString.foreach(y->System.out.println(y));
		
		
		
		/*MAP TO PAIR*/
		
		JavaPairRDD<Integer,Integer> pairrdd= intrdd.mapToPair(
				new PairFunction<Integer, Integer,Integer>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> call(Integer t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<Integer, Integer>(t,1);
					}
					
				});
		pairrdd.foreach(f->System.out.println(f));
		
	}//main

	
	
	
}
