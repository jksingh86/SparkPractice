package demo.spark;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DemoSparkJson {

	public static void main(String[] args) {
		String fileName = "/home/jitendra/Documents/mongodbdata/people-bson/people.json";
		SparkConf conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> file = sc.textFile(fileName);
		JavaPairRDD<String, Integer> words = file.flatMap(s -> Arrays.asList(s.split("\"")).iterator())
				.flatMap(s -> Arrays.asList(s.split("")).iterator())
			    .mapToPair(word -> new Tuple2<>(word, 1))
			    .reduceByKey((a, b) -> a + b)
			    .filter(a -> a._1.length()>20 && a._1.length()<50 && a._2 >1);
		
		words.saveAsTextFile("count.txt");
		sc.stop();
		}

}
