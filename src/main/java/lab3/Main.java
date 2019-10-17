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
    private static final int MAX_DELAY_POS = 0;
    private static final int IS_CANCELLED_POS = 1;
    private static final int NUMBER_OF_DELAYED_POS = 2;
    private static final int NUMBER_OF_CANCELLED_POS = 3;
    private static final int NUMBER_OF_FLIGHTS_POS = 4;


    private static JavaRDD<String> getDataFromFile(JavaSparkContext sc, String path){
        JavaRDD<String> data = sc.textFile(path).flatMap(s -> Arrays.stream(s.split("\t")).iterator());
        final String header = data.first();
        return data.filter(line -> !line.equals(header));
    }

    private static String getFromAirports(int pos, String s){
        String s_sub = s.split(",", 2)[pos];
        return s_sub.substring(1,s_sub.length() - 1);
    }

    private static String getFromSchedule(int pos, String s){
        return s.split(",")[pos];
    }

    private static String getAirportData(int pos, String s, boolean isAirports){
        if (isAirports){
            return getFromAirports(pos, s);
        } else {
            return getFromSchedule(pos, s);
        }
    }

    private static Broadcast<Map<Integer,String>> getAirportBroadcasted(JavaSparkContext sc, JavaRDD<String> airports){
        JavaPairRDD<Integer, String> airportsPair = airports.mapToPair(s -> new Tuple2<>(Integer.parseInt(getAirportData(AIRPORT_ID_POS, s, true)), getAirportData(AIRPORT_NAME_POS, s, true)));
        Map<Integer, String> airportsMap = airportsPair.collectAsMap();
        return sc.broadcast(airportsMap);
    }

    private static JavaPairRDD<Pair<Integer, Integer>, float[]> createSchedulePair(JavaRDD<String> schedule){
        return schedule.mapToPair(s -> {
            Pair<Integer, Integer> airportsIDs = new Pair<>(Integer.parseInt(getAirportData(AIRPORT_ID_FROM_POS,s,false)),Integer.parseInt(getAirportData(AIRPORT_ID_TO_POS,s,false)));
            float[] delayData = new float[]{0,0,0,0,1};
            if (getAirportData(DELAY_FLIGHT_POS, s, false).length() > 0) {
                delayData[MAX_DELAY_POS] = Float.parseFloat(getAirportData(DELAY_FLIGHT_POS,s,false));
                delayData[NUMBER_OF_DELAYED_POS] = 1;
            } else {
                delayData[IS_CANCELLED_POS] = 1;
            }
            return new Tuple2<>(airportsIDs, delayData);
        });
    }

    private static JavaPairRDD<Pair<Integer, Integer>, float[]> filterSchedule(JavaPairRDD<Pair<Integer, Integer>, float[]> schedule){
        return schedule.filter(pair -> pair._2[MAX_DELAY_POS] >= 0);
    }

    private static JavaPairRDD<Pair<Integer, Integer>, float[]> reduceSchedule(JavaPairRDD<Pair<Integer, Integer>, float[]> schedule){
        return schedule.reduceByKey((arr1,arr2) -> {
            arr1[NUMBER_OF_CANCELLED_POS] = arr1[NUMBER_OF_CANCELLED_POS] + arr1[IS_CANCELLED_POS] + arr2[IS_CANCELLED_POS] + arr2[NUMBER_OF_CANCELLED_POS];
            arr1[NUMBER_OF_DELAYED_POS] += arr2[NUMBER_OF_DELAYED_POS];
            if (arr1[MAX_DELAY_POS] <= arr2[MAX_DELAY_POS]) {
                arr1[MAX_DELAY_POS] = arr2[MAX_DELAY_POS];
            }
            arr1[NUMBER_OF_FLIGHTS_POS] += arr2[NUMBER_OF_FLIGHTS_POS];
            return arr1;
        });
    }

    private static JavaPairRDD<Pair<Integer, Integer>, String> convertDataToString(JavaPairRDD<Pair<Integer, Integer>, float[]> schedule){
        return schedule.mapValues(arr -> "Max delay=" + arr[MAX_DELAY_POS] + "; Percent of delays = " + arr[NUMBER_OF_DELAYED_POS]/arr[NUMBER_OF_FLIGHTS_POS] * 100 +
                "%; Percent of cancelled = " + arr[NUMBER_OF_CANCELLED_POS]/arr[NUMBER_OF_FLIGHTS_POS] * 100 + "%; Number of flights = " + arr[NUMBER_OF_FLIGHTS_POS]);
    }

    private static JavaRDD<String> mapAirportsIDs(JavaPairRDD<Pair<Integer, Integer>, String> schedule, final Broadcast<Map<Integer,String>> airportsBroadcasted){
        return schedule.map(data -> {
            int airportID1 = data._1.getKey();
            int airportID2 = data._1.getValue();
            String info = data._2;
            String airportName1 = airportsBroadcasted.getValue().get(airportID1);
            String airportName2 = airportsBroadcasted.getValue().get(airportID2);
            info = airportID1 + " (" + airportName1 + ") -> " + airportID2 + " (" + airportName2 + ") " + info;
            return info;
        });
    }

    private static JavaPairRDD<Pair<Integer, Integer>, String> handleSchedule(JavaRDD<String> schedule){
        JavaPairRDD<Pair<Integer, Integer>, float[]> schedulePair = createSchedulePair(schedule);
        JavaPairRDD<Pair<Integer, Integer>, float[]> scheduleFiltered = filterSchedule(schedulePair);
        JavaPairRDD<Pair<Integer, Integer>, float[]> scheduleReduced = reduceSchedule(scheduleFiltered);
        JavaPairRDD<Pair<Integer, Integer>, String> scheduleStringConverted = convertDataToString(scheduleReduced);
        return scheduleStringConverted;
    }


    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = getDataFromFile(sc, args[0]);
        JavaRDD<String> schedule = getDataFromFile(sc, args[1]);
        final Broadcast<Map<Integer,String>> airportsBroadcasted = getAirportBroadcasted(sc,airports);
        JavaPairRDD<Pair<Integer, Integer>, String> scheduleHandled = handleSchedule(schedule);
        JavaRDD<String> output = mapAirportsIDs(scheduleHandled, airportsBroadcasted);
        output.saveAsTextFile(args[2]);
    }
}
