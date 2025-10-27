package com.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

public class OrderFileReaderExample {

    // 已知店铺权重：根据订单数量大致设置
    static Map<String, Integer> shopWeights = new HashMap<>();
    static {
        shopWeights.put("Shop-A", 2); // 小店，不拆分
        shopWeights.put("Shop-B", 4); // 热点大店，拆成 4 个子 key
        shopWeights.put("Shop-C", 1); // 小店，不拆分
    }

    static Random random = new Random();

    public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // For local testing keep parallelism deterministic to avoid key-group
    // range mismatches when Flink assigns key groups. In production you
    // should set a fixed maxParallelism and avoid changing it between
    // job submissions or ensure operator parallelisms are compatible.
    // See: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/ops/state/job_savepoints/#key-groups
    env.setParallelism(1);

        // 读取 order.txt 文件
        DataStream<String> lines;
        URL resource = OrderFileReaderExample.class.getResource("/order.txt");
        if (resource == null) {
            throw new RuntimeException("order.txt 未在 classpath 中找到。请放到 src/main/resources/");
        }

        String protocol = resource.getProtocol();
        if ("file".equals(protocol)) {
            lines = env.readTextFile(resource.getPath());
        } else {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    OrderFileReaderExample.class.getResourceAsStream("/order.txt"), StandardCharsets.UTF_8))) {
                List<String> list = br.lines().collect(Collectors.toList());
                lines = env.fromCollection(list);
            }
        }

        // 解析每行 -> (店铺名, 订单总额)
        DataStream<Tuple2<String, Integer>> orders = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                try {
                    String[] fields = line.split(",");
                    if (fields.length != 4) return;

                    String shopName = fields[1].trim();
                    int price = Integer.parseInt(fields[2].trim());
                    int quantity = Integer.parseInt(fields[3].trim());
                    int total = price * quantity;

                    out.collect(Tuple2.of(shopName, total));
                } catch (Exception e) {
                    System.err.println("解析失败: " + line);
                }
            }
        });

        // 在子 key 聚合前记录分配情况（为排查/调试）
        DataStream<Tuple2<String, Integer>> partialSum = orders
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> order) throws Exception {
                        int weight = shopWeights.getOrDefault(order.f0, 1);
                        if (weight > 1) {
                            int prefix = random.nextInt(weight); // 拆分到多个子 key
                            String subKey = prefix + "_" + order.f0;
                            // 在这里打印分配到的子 key，便于调试子 key 聚合前的分布
                            System.out.println(String.format("[Pre-agg] 分配子 key: %s, 原店铺: %s, 金额: %d", subKey, order.f0, order.f1));
                            return Tuple2.of(subKey, order.f1);
                        } else {
                            // 对于不拆分的店铺，也打印一条信息以便观察
                            System.out.println(String.format("[Pre-agg] 未拆分, 使用原 key: %s, 金额: %d", order.f0, order.f1));
                            return order;
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);

        // 去掉前缀做全局聚合
    // Use an explicit MapFunction implementation to avoid Java type-erasure
    // issues when returning a Tuple2 from a lambda. This makes Flink's
    // type extractor able to determine the generic types.
    DataStream<Tuple2<String, Integer>> finalSum = partialSum
        .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> t) throws Exception {
            String shop = t.f0.contains("_") ? t.f0.substring(t.f0.indexOf("_") + 1) : t.f0;
            return Tuple2.of(shop, t.f1);
            }
        })
        .keyBy(t -> t.f0)
        .sum(1);

        // 输出结果
        finalSum.map(t -> String.format("店铺: %s 总额: %d", t.f0, t.f1)).print();

        env.execute("Weighted Key Balanced Order Aggregation");
    }
}
