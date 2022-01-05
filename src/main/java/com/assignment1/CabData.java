package com.assignment1;
import java.io.File;
import java.lang.Iterable;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;


public class CabData {

    public static void main(String[] args) throws Exception {
        String dir = "/Users/ekwong/Downloads/flink_sandbox/assignments/cab_analysis/output/";

        final ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();

        DataSet< String > data = env.readTextFile("/Users/ekwong/Downloads/flink_sandbox/assignments/cab_analysis/cabflink.txt");

        DataSet <Tuple8 < String, String, String, String, Boolean, String, String, Integer >> mapped =
                        data.map(new Splitter());
        DataSet <Tuple8 < String, String, String, String, Boolean, String, String, Integer >> filter = mapped.filter(new FilterOperation());


        DataSet < Tuple8 < String, String, String, String, Boolean, String, String, Integer >> popularDest = filter.groupBy(6).sum(7).maxBy(7);

        popularDest.writeAsText(dir + "popularDest").setParallelism(1);
        new File(dir + "popularDest").delete();



        // avg. passengers per trip source: place to pickup most passengers
        DataSet < Tuple2 < String, Double >> avgPassPerTrip = filter
                .map(new MapFunction < Tuple8 < String, String, String, String, Boolean, String, String, Integer > , Tuple3 < String, Integer, Integer >> () {
                    public Tuple3 < String, Integer, Integer > map(Tuple8 < String, String, String, String, Boolean, String, String, Integer > value) {
                        // driver,trip_passengers,trip_count
                        return new Tuple3 < String, Integer, Integer > (value.f5, value.f7, 1);
                    }
                })
                .groupBy(f->f.f0)
                .reduce(new ReduceFunction < Tuple3 < String, Integer, Integer >> () {
                    public Tuple3 < String, Integer, Integer > reduce(Tuple3 < String, Integer, Integer > v1, Tuple3 < String, Integer, Integer > v2) {
                        return new Tuple3 < String, Integer, Integer > (v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                    }
                })
                .map(new MapFunction < Tuple3 < String, Integer, Integer > , Tuple2 < String, Double >> () {
                    public Tuple2 < String, Double > map(Tuple3 < String, Integer, Integer > value) {
                        return new Tuple2 < String, Double > (value.f0, ((value.f1 * 1.0) / value.f2));
                    }
                });
        new File(dir + "avg_passengers_per_trip.txt").delete();
        avgPassPerTrip.writeAsText(dir + "avg_passengers_per_trip.txt").setParallelism(1);

        DataSet<Tuple2<String, Double>> avgTripsPerDriver = filter
                .map(new MapFunction < Tuple8 < String, String, String, String, Boolean, String, String, Integer > , Tuple3 < String, Integer, Integer >> () {
                    public Tuple3 < String, Integer, Integer > map(Tuple8 < String, String, String, String, Boolean, String, String, Integer > value) {
                        // driver,trip_passengers,trip_count
                        return new Tuple3 < String, Integer, Integer > (value.f3, value.f7, 1);
                    }
                })
                .groupBy(f->f.f0)
                        .reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
                            @Override
                            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> v1, Tuple3<String, Integer, Integer> v2) throws Exception {
                                return new Tuple3 < String, Integer, Integer > (v1.f0, v1.f1 + v2.f1, v1.f2 + v2.f2);
                            }
                        })
                .map(new MapFunction < Tuple3 < String, Integer, Integer > , Tuple2 < String, Double >> () {
                    public Tuple2 < String, Double > map(Tuple3 < String, Integer, Integer > value) {
                        return new Tuple2 < String, Double > (value.f0, ((value.f1 * 1.0) / value.f2));
                    }
                });
        new File(dir + "avg_trips_per_driver.txt").delete();
        avgTripsPerDriver.writeAsText(dir + "avg_trips_per_driver.txt").setParallelism(1);
        env.execute("Cab analysis");


    }

    public static class FilterOperation implements FilterFunction<Tuple8 < String, String, String, String, Boolean, String, String, Integer>>{

        public boolean filter(Tuple8 < String, String, String, String, Boolean, String, String, Integer> value){
            return value.f4;
        }
    }

    public static class Splitter implements MapFunction<String, Tuple8<String, String, String, String, Boolean, String, String, Integer>>
    {
        public Tuple8<String, String, String, String, Boolean, String, String, Integer> map(String value)
        {
            String[] words = value.split(",");

            if (words[4].equalsIgnoreCase("yes"))
                return new Tuple8 < String, String, String, String, Boolean, String, String, Integer > (words[0], words[1], words[2], words[3], true, words[5], words[6], Integer.parseInt(words[7]));

            return new Tuple8 < String, String, String, String, Boolean, String, String, Integer > (words[0], words[1], words[2], words[3], false, words[5], words[6], 0);


        }
    }



}
