package com.example.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

public class OrderFileReaderExample {

    public static void main(String[] args) throws Exception {
        // 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从资源或文件读取订单数据。实现策略：
        // 1) 尝试通过 classpath 加载 resource (/order.txt)
        // 2) 如果 classpath 资源是 file 协议（开发时通常在 target/classes 下），把它的路径交给 readTextFile
        // 3) 如果资源在 jar 中（例如打成 fat-jar），通过流读取全部行并使用 env.fromCollection
        DataStream<String> lines;
        URL resource = OrderFileReaderExample.class.getResource("/order.txt");
        if (resource == null) {
            throw new RuntimeException("order.txt 未在 classpath 中找到。请将 order.txt 放到 src/main/resources 或提供文件系统路径。");
        }

        String protocol = resource.getProtocol();
        if ("file".equals(protocol)) {
            // 在开发环境或 exploded classes 下可以直接传文件系统路径给 readTextFile
            lines = env.readTextFile(resource.getPath());
        } else {
            // 例如 resource 在 jar 中（protocol=jar），读取为流并用 fromCollection
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    OrderFileReaderExample.class.getResourceAsStream("/order.txt"), StandardCharsets.UTF_8))) {
                List<String> list = br.lines().collect(Collectors.toList());
                lines = env.fromCollection(list);
            }
        }

        // 解析每一行 -> (店铺名, 订单总额)
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

        // 按店铺汇总总额
        orders
            .keyBy(t -> t.f0)
            .sum(1)
            .map(t -> String.format("店铺: %s 总额: %d", t.f0, t.f1))
            .print();

        // 启动作业
        env.execute("Order File Reader Example");
    }
}
