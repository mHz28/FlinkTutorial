package com.assignment1;
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

        final ExecutionEnvironment env   = ExecutionEnvironment.getExecutionEnvironment();

        DataSet< String > data = env.readTextFile("/Users/ekwong/Downloads/flink_sandbox/assignments/cab_analysis/cabflink.txt");

        DataSet <Tuple8 < String, String, String, String, Boolean, String, String, Integer >> mapped =
                        data.map(new Splitter());
        DataSet <Tuple8 < String, String, String, String, Boolean, String, String, Integer >> filter = mapped.filter(new FilterOperation());


        DataSet < Tuple8 < String, String, String, String, Boolean, String, String, Integer >> popularDest = filter.groupBy(6).sum(7).maxBy(7);

        popularDest.writeAsText("/Users/ekwong/Downloads/flink_sandbox/assignments/cab_analysis/output/popularDest").setParallelism(1);

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
