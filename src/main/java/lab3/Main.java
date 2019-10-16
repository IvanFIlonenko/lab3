package lab3;

import javafx.util.Pair;
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
        JavaRDD<String> airports = sc.textFile(args[0]).flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        JavaRDD<String> schedule = sc.textFile(args[1]).flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        JavaPairRDD<Integer, String> airportsPair = airports.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(",",2)[0]), s.split(",",2)[1]));
        JavaPairRDD<Pair<Integer, Integer>, > SchedulePair = schedule.mapToPair(s -> new Tuple2<>(new Pair<>(Integer.parseInt(s.split(",")[11]),Integer.parseInt(s.split(",")[14])), s.split(",")[17]));
        splittedCount.saveAsTextFile(args[2]);
    }
}
