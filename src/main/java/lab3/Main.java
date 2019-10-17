package lab3;

import javafx.util.Pair;
import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;
import scala.Tuple2;

import java.util.*;

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
        JavaPairRDD<Pair<Integer, Integer>, float[]> schedulePair = schedule.mapToPair(s -> {
            if (s.split(",")[17].length() > 0) {
                return new Tuple2<>(new Pair<>(Integer.parseInt(s.split(",")[11]),Integer.parseInt(s.split(",")[14])), new float[]{Float.parseFloat(s.split(",")[17]),0,1,0,1});
            } else {
                return new Tuple2<>(new Pair<>(Integer.parseInt(s.split(",")[11]),Integer.parseInt(s.split(",")[14])), new float[]{0,1,0,0,1});
            }
        });
        schedulePair = schedulePair.filter(pair -> pair._2[0] >= 0);
        schedulePair = schedulePair.reduceByKey((arr1,arr2) -> {
            arr1[3] = arr1[3] + arr1[1] + arr2[1];
            if (arr2[1] == 0 && arr2[0] > 0) {
                arr1[2] += 1;
            }
            if (arr1[0] <= arr2[0]) {
                arr1[0] = arr2[0];
            }
            arr1[4] += arr2[4];
            return arr1;
        });
        JavaPairRDD<Pair<Integer, Integer>, String> scheduleData = schedulePair.mapValues(arr -> "Max delay=" + arr[0] + "; Percent of delays = " + arr[2]/arr[4] * 100 + "%; Percent of cancelled = " + arr[3]/arr[4] * 100 + "%;" + arr[4]);
        Map<Integer, String> kek = airportsPair.collectAsMap();
        final Broadcast<Map<Integer,String>> airportsBroadcasted = sc.broadcast(kek);
        JavaRDD<String> output = scheduleData.map(data -> {
            int airportID1 = data._1.getKey();
            int airportID2 = data._1.getValue();
            String info = data._2;
            String airportName1 = airportsBroadcasted.getValue().get(airportID1);
            String airportName2 = airportsBroadcasted.getValue().get(airportID2);
            info = airportID1 + " ( " + airportName1 + " ) -> " + airportID2 + " ( " + airportName2 + " ) " + info;
            return info;
        });
        output.saveAsTextFile(args[2]);
    }
}
