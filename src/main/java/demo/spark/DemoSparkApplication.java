package demo.spark;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DemoSparkApplication {

	 public static void main(String[] args) {
	        // configure spark
	        SparkSession spark = SparkSession
	                .builder()
	                .appName("Spark Example - Read JSON to RDD")
	                .master("local[2]")
	                .getOrCreate();
	 
	        // read list to RDD
	        String jsonPath = "/home/jitendra/Documents/mongodbdata/companies.json";
	        JavaRDD<Row> items = spark.read().json(jsonPath).toJavaRDD();
	 
	        items.foreach(item -> {
	            System.out.println(item); 
	        });
	    } 
	
	
	/*
	 * public static void main(String[] args) { String fileName =
	 * "/home/jitendra/Documents/mongodbdata/companies.json"; SparkConf conf = new
	 * SparkConf().setAppName("SparkWordCount").setMaster("local[2]");
	 * JavaSparkContext sc = new JavaSparkContext(conf); SparkSession spark =
	 * SparkSession.builder().appName("ProcessJSONData")
	 * .master("local").getOrCreate();
	 * 
	 * 
	 * SparkSession spark = SparkSession .builder() .appName("SparkWordCount")
	 * .getOrCreate();
	 * 
	 * Dataset<Row> people = spark.read().json(fileName); people.printSchema();
	 * 
	 * people.createOrReplaceTempView("people");
	 * 
	 * // SQL statements can be run by using the sql methods provided by spark
	 * Dataset<Row> namesDF = spark.sql("SELECT Name FROM people"); namesDF.show();
	 * //--------------------
	 * 
	 * 
	 * //Encoders are created for Java bean class Encoder<Map> fruitEncoder =
	 * Encoders.bean(Map.class);
	 * 
	 * Dataset<Map> fruitDS = spark.read().json(fileName).as(fruitEncoder);
	 * 
	 * fruitDS.show();
	 * 
	 * //-----------------
	 * 
	 * sc.stop(); }
	 */

}
