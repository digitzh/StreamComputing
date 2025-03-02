package org.MDGA.P4_KeyByAndReduce.src;

import stream.DataStream;
import stream.source.KafkaSource;
import stream.sink.KafkaSink;

/**
 * Kafka流处理示例
 * 本示例展示了如何：
 * 1. 从Kafka读取数据
 * 2. 使用KeyBy和Reduce进行数据处理
 * 3. 将结果写回Kafka
 */
public class KafkaStreamExample {
    public static void main(String[] args) {
        // Kafka配置
        String bootstrapServers = "localhost:9092";
        String inputTopic = "input-topic";
        String outputTopic = "output-topic";
        String groupId = "stream-processor-group";

        // 步骤1：创建Kafka数据源
        // 配置从Kafka读取字符串并转换为整数
        KafkaSource<Integer> source = new KafkaSource<>(
            bootstrapServers,
            inputTopic,
            groupId,
            Integer::parseInt, // 将Kafka中的字符串转换为整数
            100 // 批处理大小
        );

        // 步骤2：创建数据流并配置处理逻辑
        DataStream<Integer> dataStream = source.getDataStream()
            .setParallelism(2)
            .withWindow(1000);

        // 步骤3：使用KeyBy和Reduce进行数据处理
        // 按照奇偶性对数据分组并求和
        DataStream<Integer> result = dataStream
            .keyBy(num -> num % 2 == 0 ? "偶数" : "奇数")
            .reduce(Integer::sum);

        // 步骤4：创建Kafka输出目标
        // 配置将整数转换为字符串后写入Kafka
        KafkaSink<Integer> sink = new KafkaSink<>(
            bootstrapServers,
            outputTopic,
            String::valueOf // 将整数转换为字符串
        );

        // 步骤5：将处理结果写入Kafka
        try {
            result.getData().forEach(sink::write);
        } finally {
            // 关闭资源
            source.close();
            sink.close();
        }
    }
}