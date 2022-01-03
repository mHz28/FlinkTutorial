package com.tutorial;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Aggregation {

    public static void main(String[] args) throws Exception
    {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.readTextFile("/Users/ekwong/Downloads/flink_sandbox/aggregation/avg1");

        // month, category,product, profit,
        DataStream<Tuple4<String, String, String, Integer>> mapped = data.map(new Splitter());      // tuple  [June,Category5,Bat,12]
        //       [June,Category4,Perfume,10,1]
        mapped.keyBy(t->t.f0).sum(3).writeAsText("/Users/ekwong/Downloads/flink_sandbox/aggregation/output/out1").setParallelism(1);

        mapped.keyBy(t->t.f0).min(3).writeAsText("/Users/ekwong/Downloads/flink_sandbox/aggregation/output/out2").setParallelism(1);

        mapped.keyBy(t->t.f0).minBy(3).writeAsText("/Users/ekwong/Downloads/flink_sandbox/aggregation/output/out3").setParallelism(1);

        mapped.keyBy(t->t.f0).max(3).writeAsText("/Users/ekwong/Downloads/flink_sandbox/aggregation/output/out4").setParallelism(1);

        mapped.keyBy(t->t.f0).maxBy(3).writeAsText("/Users/ekwong/Downloads/flink_sandbox/aggregation/output/out5").setParallelism(1);
        // execute program
        env.execute("Aggregation");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class Splitter implements MapFunction<String, Tuple4<String, String, String, Integer>>
    {
        public Tuple4<String, String, String, Integer> map(String value)         // 01-06-2018,June,Category5,Bat,12
        {
            String[] words = value.split(",");                             // words = [{01-06-2018},{June},{Category5},{Bat}.{12}
            // ignore timestamp, we don't need it for any calculations
            return new Tuple4<String, String, String, Integer>(words[1], words[2],	words[3], Integer.parseInt(words[4]));
        }                                                  //    June    Category5      Bat               12
    }
}

