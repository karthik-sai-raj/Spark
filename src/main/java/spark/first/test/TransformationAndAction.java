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
import org.apache.spark.api.java.function.Function2;
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
		System.out.println("MAP +y");
		stringexp.foreach(y->System.out.println(y));
		
		/*python map()*
		 * 
		 * x = sc.parallelize(["b", "a", "c"]) 
			y = x.map(lambda z: (z, 1))
			print(x.collect())
			print(y.collect())

		 * 
		 * /
		
		
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
		/*Flatmap()
		 * 
		 *x = sc.parallelize([1,2,3]) 
			y = x.flatMap(lambda x: (x, x*100, 42))
			print(x.collect())
			print(y.collect())
 
		 * 
		 * */
		
		
		/*FILTER*/
		
		JavaRDD<Integer> GreaterThatFive =sc.parallelize(Arrays.asList(5,58,2,65,6,4));
		
		JavaRDD<Integer> result =GreaterThatFive.filter(x->(x>5));
		
		JavaRDD<String> filterString=fmrdd2.filter(x->   x.startsWith("d")|| x.startsWith("A"));//using flat map here fmrdd2
		
		result.foreach(x->System.out.println(x));
		
		filterString.foreach(y->System.out.println(y));
		/*python filter()
		 * x = sc.parallelize([1,2,3])
			y = x.filter(lambda x: x%2 == 1) #keep odd values
			print(x.collect())
			print(y.collect())
		 * */
		
		
		/*MAP TO PAIR*/
		
		JavaPairRDD<String,Integer> pairrdd= fmrdd2.mapToPair(
				new PairFunction<String, String,Integer>(){

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String t) throws Exception {
						// TODO Auto-generated method stub
						return new Tuple2<String, Integer>(t,1);
					}
					
				});
		pairrdd.foreach(f->System.out.println(f));
		
		
		
		/*Reduce by key*/
		System.out.println("Reduce by key");
		JavaPairRDD<String, Integer> rbk= pairrdd.reduceByKey(new Function2<Integer,Integer,Integer>(){
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						// TODO Auto-generated method stub
						System.out.println(v1+" / "+v2);
						return v1+v2;
					}
					
				});
		rbk.foreach(x->System.out.println(x));
		
	/*sort by key*/
		System.out.println("sort by key ");
		JavaPairRDD<String, Integer> sbk=rbk.sortByKey();//false decending order
		sbk.foreach(x -> System.out.println(x));
		
		
		
		/*SAMPLE*/
		JavaPairRDD<String, Integer> sample=rbk.sample(false, 1, 3);
		System.out.println("Sample");
		sample.foreach(x -> System.out.println(x));
		
		
		
		/*randomSplit*/
		
		//JavaPairRDD<String ,Integer>[] splits=rbk.randomSplit(new double[] {0.6,0.4}, 11l);
		
		/*union*/
		System.out.println("UNION OPERATION SET THEORY");
		JavaRDD<Integer> example =sc.parallelize(Arrays.asList(6,7,8,9,0));
		JavaRDD<Integer> union= example.union(intrdd);
		union.foreach(x -> System.out.println(x));
		
		
		
	}//main 

	
	
	
}
