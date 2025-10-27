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
                Tuple2.of("Shop-A", 1),
                Tuple2.of("Shop-B", 20),
                Tuple2.of("Shop-C", 2)
        );

        DataStream<Tuple2<String, Integer>> orders = input.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            Random random = new Random();

            @Override
            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String shop = value.f0;
                int count = value.f1;
                for (int i = 0; i < count; i++) {
                    int amount = random.nextInt(101); // 0..100
                    System.out.printf("Order -> shop=%s amount=%d%n", shop, amount);
                    out.collect(Tuple2.of(shop, amount));
                }
            }
        });

        // Aggregate total amount per shop （ Upsert ）
        orders
        .keyBy(t -> t.f0)
        .sum(1)
        .map(t -> String.format("店铺: %s 总额: %d", t.f0, t.f1))
        .print();

        env.execute("Salted Weighted Split Example");
    }
}
