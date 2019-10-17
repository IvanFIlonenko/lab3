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
    private static final int AIRPORT_ID_POS= 0;
    private static final int AIRPORT_NAME_POS = 1;
    private static final int AIRPORT_ID_FROM_POS = 11;
    private static final int AIRPORT_ID_TO_POS = 14;
    private static final int DELAY_FLIGHT_POS = 17;

    public static JavaRDD<String> getDataFromFile(JavaSparkContext sc, String path){
        JavaRDD<String> data = sc.textFile(path).flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        final String header = data.first();
        return data.filter(line -> !line.equals(header));
    }

    public static Broadcast<Map<Integer,String>> getAirportBroadcasted(JavaSparkContext sc, JavaRDD<String> airports){
        JavaPairRDD<Integer, String> airportsPair = airports.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.split(",",2)[0].substring(1, s.split(",",2)[0].length() - 1)), s.split(",",2)[1]));
        Map<Integer, String> airportsMap = airportsPair.collectAsMap();
        return sc.broadcast(airportsMap);
    }


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = getDataFromFile(sc, args[0]);
        JavaRDD<String> schedule = getDataFromFile(sc, args[1]);
        final Broadcast<Map<Integer,String>> airportsBroadcasted = getAirportBroadcasted(sc,airports);
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
        JavaRDD<String> output = scheduleData.map(data -> {
            int airportID1 = data._1.getKey();
            int airportID2 = data._1.getValue();
            String info = data._2;
            String airportName1 = airportsBroadcasted.getValue().get(airportID1);
            String airportName2 = airportsBroadcasted.getValue().get(airportID2);
            info = airportID1 + " (" + airportName1 + ") -> " + airportID2 + " (" + airportName2 + ") " + info;
            return info;
        });
        output.saveAsTextFile(args[2]);
    }
}
