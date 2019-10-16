package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class Main {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile("L_AIRPORT_ID.csv");
        JavaRDD<String> schedule = sc.textFile("664600583_T_ONTIME_sample.csv");
        JavaRDD<String> splitted = airports.flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        splitted.saveAsTextFile("output.txt");
    }
}
