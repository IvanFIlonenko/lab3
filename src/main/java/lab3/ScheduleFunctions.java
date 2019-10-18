package lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class ScheduleFunctions {
    private static final int AIRPORT_ID_FROM_POS = 11;
    private static final int AIRPORT_ID_TO_POS = 14;
    private static final int DELAY_FLIGHT_POS = 17;
    private static final int MAX_DELAY_POS = 0;
    private static final int IS_CANCELLED_POS = 1;
    private static final int NUMBER_OF_DELAYED_POS = 2;
    private static final int NUMBER_OF_CANCELLED_POS = 3;
    private static final int NUMBER_OF_FLIGHTS_POS = 4;


    public static String getFromSchedule(int pos, String s){
        return s.split(",")[pos];
    }

    public static JavaPairRDD<Pair<Integer, Integer>, String> handleSchedule(JavaRDD<String> schedule){
        JavaPairRDD<Pair<Integer, Integer>, float[]> schedulePair = createSchedulePair(schedule);
        JavaPairRDD<Pair<Integer, Integer>, float[]> scheduleFiltered = filterSchedule(schedulePair);
        JavaPairRDD<Pair<Integer, Integer>, float[]> scheduleReduced = reduceSchedule(scheduleFiltered);
        JavaPairRDD<Pair<Integer, Integer>, String> scheduleStringConverted = convertDataToString(scheduleReduced);
        return scheduleStringConverted;
    }

    private static JavaPairRDD<Pair<Integer, Integer>, float[]> createSchedulePair(JavaRDD<String> schedule){
        return schedule.mapToPair(s -> {
            Pair<Integer, Integer> airportsIDs = new Pair<>(Integer.parseInt(AirportsFunctions.getAirportData(AIRPORT_ID_FROM_POS,s,false)),Integer.parseInt(AirportsFunctions.getAirportData(AIRPORT_ID_TO_POS,s,false)));
            float[] delayData = new float[]{0,0,0,0,1};
            if (AirportsFunctions.getAirportData(DELAY_FLIGHT_POS, s, false).length() > 0) {
                delayData[MAX_DELAY_POS] = Float.parseFloat(AirportsFunctions.getAirportData(DELAY_FLIGHT_POS,s,false));
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
}
