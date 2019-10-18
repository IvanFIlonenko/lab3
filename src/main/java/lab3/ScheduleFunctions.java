package lab3;

import javafx.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class ScheduleFunctions {

    public static String getFromSchedule(int pos, String s){
        return s.split(",")[pos];
    }

    private static JavaPairRDD<Pair<Integer, Integer>, String> handleSchedule(JavaRDD<String> schedule){
        JavaPairRDD<Pair<Integer, Integer>, float[]> schedulePair = createSchedulePair(schedule);
        JavaPairRDD<Pair<Integer, Integer>, float[]> scheduleFiltered = filterSchedule(schedulePair);
        JavaPairRDD<Pair<Integer, Integer>, float[]> scheduleReduced = reduceSchedule(scheduleFiltered);
        JavaPairRDD<Pair<Integer, Integer>, String> scheduleStringConverted = convertDataToString(scheduleReduced);
        return scheduleStringConverted;
    }
}
