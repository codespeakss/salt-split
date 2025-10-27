package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Random;

public class SaltedWeightedSplitExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("userA", 1),
                Tuple2.of("userB", 5),
                Tuple2.of("userC", 3)
        );

        DataStream<Tuple2<String, Integer>> salted = input.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            Random random = new Random();

            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                int weight = value.f1;
                for (int i = 0; i < weight; i++) {
                    int salt = random.nextInt(weight);
                    out.collect(Tuple2.of(value.f0 + "_" + salt, value.f1));
                }
            }
        });

        salted
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute("Salted Weighted Split Example");
    }
}
