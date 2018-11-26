import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.List;
public class BarriosBCNSexo {
	static Logger log = Logger.getLogger(BarriosBCNSexo.class.getName());
	
	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF) ;

		// STEP 1: create a SparkConf object
		if (args.length < 1) {
			log.fatal("Syntax Error: there must be one argument (a file name or a directory)")  ;
			throw new RuntimeException();
		}

		// STEP 2: create a SparkConf object
		SparkConf sparkConf = new SparkConf().setAppName("Viviendas BCN").setMaster("local");
		sparkConf.set("spark.driver.memory", "471859200");

		// STEP 3: create a Java Spark context
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

		// STEP 4: read lines of files
		JavaRDD<String> data = sparkContext.textFile(args[0]);
		
		// STEP 5: split the lines into parts
		JavaRDD<List<String>> dataMapped = data.map(line -> Arrays.asList(line.split(",")));
		
		// STEP 6: filter lines, only country Spain
		JavaRDD<List<String>> dataReduce = dataMapped.filter(line -> line.get(1).equals("3"));
		
		// STEP 7: Set line in pairs of key-value
		JavaPairRDD<String, Integer> pairs = dataReduce.mapToPair(line -> new Tuple2<>(line.get(5), 1)) ;
		
		// STEP 8: Reduce lines  
		JavaPairRDD<String, Integer> groupedPairs = pairs.reduceByKey((String, integer2) -> String + integer2);

		// STEP 9: Sort the results by key ant take the elements
		List<Tuple2<String, Integer>> output = groupedPairs
		        .sortByKey(true).take(99999);

		// STEP 10: print the results
		for (Tuple2<?, ?> tuple : output) 
		{
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		// STEP 11: save results in a file
		//groupedPairs.saveAsTextFile("outputfile");
		
		System.out.println("Program finished");
		
		// STEP 12: stop the spark context
		sparkContext.stop();
	}

}
