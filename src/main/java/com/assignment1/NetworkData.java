package com.assignment1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.HashSet;


public class NetworkData {
    public static void main(String[] args) throws Exception {
        String dir = "/Users/ekwong/Downloads/flink_sandbox/assignments/network_data/";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile(dir + "ip-data.txt");


        DataStream<Tuple2<String, String>> keyData = data.map(new KeySplit());

        DataStream<Tuple2<String, String>> usCountries = keyData.filter(new USFilter());

        DataStream<Tuple2<String, Integer>> websiteClicks = usCountries.map(new WebsiteMap())
                .keyBy(f->f.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .sum(2)
                .map(new WebsiteClicks());
        new File(dir + "output/us_web_clicks.txt").delete();
        websiteClicks.writeAsText(dir + "output/us_web_clicks.txt").setParallelism(1);

        DataStream<Tuple2<String, Integer>> maxClicks = websiteClicks
                .keyBy(t->t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .maxBy(1);
        new File(dir + "output/us_max_clicks.txt").delete();
        maxClicks.writeAsText(dir + "output/us_max_clicks.txt").setParallelism(1);

        DataStream<Tuple2<String, Integer>> minClicks = websiteClicks
                .keyBy(t->t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .minBy(1);
        new File(dir + "output/us_min_clicks.txt").delete();
        minClicks.writeAsText(dir + "output/us_min_clicks.txt").setParallelism(1);


        DataStream<Tuple2<String, Integer>> distinctUsers = usCountries
                .keyBy(t->t.f0)
                .flatMap(new DistinctUsers());

        new File(dir + "output/distinct_users.txt").delete();
        distinctUsers.writeAsText(dir + "output/distinct_users.txt").setParallelism(1);


        DataStream< Tuple2<String, Integer>> averageTime = usCountries.map(new AverageTimeList())
                .keyBy(t->t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(5)))
                .reduce(new ReduceTime())
                .map(new CalculateAverageTime());

        new File(dir + "output/avg_time.txt").delete();
        distinctUsers.writeAsText(dir + "output/avg_time.txt").setParallelism(1);
        env.execute("Network Data");

    }

    public static class CalculateAverageTime implements  MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>{
        public Tuple2<String, Integer> map(Tuple3<String, Integer, Integer> value){
            return new Tuple2<String, Integer>(value.f0, (value.f2/value.f1));
        }
    }
    public static class ReduceTime implements ReduceFunction<Tuple3<String, Integer, Integer>>{
        public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> v1,
                                                       Tuple3<String, Integer, Integer> v2)
        {
            return new Tuple3<String, Integer, Integer>(v1.f0, v1.f1+v2.f1, v1.f2+v2.f2);
        }

    }
    public static class DistinctUsers extends RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, Integer>> {
        public transient ListState<String> userState;

        public void flatMap(Tuple2<String, String> input, Collector<Tuple2<String, Integer>> out) throws Exception{
            userState.add(input.f1);

            HashSet<String> distinctUsers = new HashSet<String>();
            for (String user: userState.get())
            {
                distinctUsers.add(user);

            }
            out.collect(new Tuple2<String, Integer>(input.f0, distinctUsers.size()));

        }

        public void open(Configuration config)
        {
            ListStateDescriptor<String> desc = new ListStateDescriptor<String>("users_state", BasicTypeInfo.STRING_TYPE_INFO);
            userState = getRuntimeContext().getListState(desc);

        }
    }

    public static class AverageTimeList implements MapFunction<Tuple2<String, String>, Tuple3<String, Integer, Integer>>{
        public Tuple3<String, Integer, Integer> map(Tuple2<String, String> value){
            int timeSpent = Integer.parseInt(value.f1.split(",")[5]);

            return new Tuple3<String, Integer, Integer>(value.f0, 1, timeSpent);
        }
    }

    public static class WebsiteClicks implements MapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>{
        public Tuple2<String, Integer> map(Tuple3<String, String, Integer> value){
            return new Tuple2<String, Integer>(value.f0, value.f2);
        }
    }

    public static class WebsiteMap implements MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>{
        public Tuple3<String, String, Integer> map(Tuple2<String, String> value){
            return new Tuple3<String, String, Integer>(value.f0, value.f1, 1);
        }
    }

    public static class USFilter implements FilterFunction<Tuple2<String, String>>{
        public boolean filter(Tuple2<String, String> value){
            String country = value.f1.split(",")[3];
            return country.equals("US");
        }
    }


    public static class KeySplit implements MapFunction<String, Tuple2<String, String>> {
        public Tuple2<String, String> map(String value){
            String fields[] = value.split(",");

            return new Tuple2<String, String>(fields[4], value);
        }
    }
}
