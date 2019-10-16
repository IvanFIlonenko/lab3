package lab3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile(args[0]);
        JavaRDD<String> schedule = sc.textFile(args[1]);
        JavaRDD<String> splitted = airports.flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        int count = 0;
        JavaPairRDD<Integer, String> splittedCount = splitted.mapToPair(s -> new Tuple2<>(count++, s));
        airports.saveAsTextFile(args[2]);
    }
}
