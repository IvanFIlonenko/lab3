package lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class ScheduleFunctions {

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
}
