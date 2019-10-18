package lab3;

import javafx.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
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
    

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = getDataFromFile(sc, args[0]);
        JavaRDD<String> schedule = getDataFromFile(sc, args[1]);
        final Broadcast<Map<Integer,String>> airportsBroadcasted = AirportsFunctions.getAirportBroadcasted(sc,airports);
        JavaPairRDD<Pair<Integer, Integer>, String> scheduleHandled = ScheduleFunctions.handleSchedule(schedule);
        JavaRDD<String> output = mapAirportsIDs(scheduleHandled, airportsBroadcasted);
        output.saveAsTextFile(args[2]);
    }
}
