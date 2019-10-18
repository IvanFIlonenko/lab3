package lab3;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class AirportsFunctions {
    private static final int AIRPORT_ID_POS= 0;
    private static final int AIRPORT_NAME_POS = 1;

    public static Broadcast<Map<Integer,String>> getAirportBroadcasted(JavaSparkContext sc, JavaRDD<String> airports){
        JavaPairRDD<Integer, String> airportsPair = airports.mapToPair(s -> new Tuple2<>(Integer.parseInt(getAirportData(AIRPORT_ID_POS, s, true)), getAirportData(AIRPORT_NAME_POS, s, true)));
        Map<Integer, String> airportsMap = airportsPair.collectAsMap();
        return sc.broadcast(airportsMap);
    }

    public static String getAirportData(int pos, String s, boolean isAirports){
        if (isAirports){
            return getFromAirports(pos, s);
        } else {
            return ScheduleFunctions.getFromSchedule(pos, s);
        }
    }

    private static String getFromAirports(int pos, String s){
        String s_sub = s.split(",", 2)[pos];
        return s_sub.substring(1,s_sub.length() - 1);
    }
}
