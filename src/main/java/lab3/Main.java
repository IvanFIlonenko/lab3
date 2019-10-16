package lab3;

import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class Main {
    public static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch(NumberFormatException e){
            return false;
        }
    }

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile(args[0]).flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        final String header1 = airports.first();
        airports = airports.filter(line -> !line.equals(header1));
        JavaRDD<String> schedule = sc.textFile(args[1]).flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        final String header2 = schedule.first();
        schedule = schedule.filter(line -> !line.equals(header2));
        JavaPairRDD<Integer, String> airportsPair = airports.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(",",2)[0]), s.split(",",2)[1]));
        JavaPairRDD<Pair<Integer, Integer>, int[]> schedulePair = schedule.mapToPair(s -> {
            if (s.split(",")[20].length() > 0) {
                return new Tuple2<>(new Pair<>(Integer.parseInt(s.split(",")[11]),Integer.parseInt(s.split(",")[14])), new int[]{0,1,0,0});
            } else {
                return new Tuple2<>(new Pair<>(Integer.parseInt(s.split(",")[11]),Integer.parseInt(s.split(",")[14])), new int[]{Integer.parseInt(s.split(",")[17]),0,0,0});
            }
        });
        long count = schedulePair.count();
//        schedulePair = schedulePair.filter(pair -> pair._2[0] < 0);
//        schedulePair = schedulePair.reduceByKey((arr1,arr2) -> {
//            arr1[3] = arr1[3] + arr1[1] + arr2[1];
//            if (arr1[1] == 0 && arr1[0] > 0) {
//                arr1[2] += 1;
//            }
//            if (arr2[1] == 0 && arr2[0] > 0) {
//                arr1[2] += 1;
//            }
////            if (arr1[0] <= arr2[0]) {
////                arr1[0] = arr2[0];
////            }
//            return arr1;
//        });
        JavaPairRDD<Pair<Integer, Integer>, String> output = schedulePair.mapValues(arr -> "Max delay=" + arr[0] + "; Percent of delays = " + arr[1] + "; Percent of cancelled = " + arr[3] + ";" + count);
//
        output.saveAsTextFile(args[2]);
    }
}
