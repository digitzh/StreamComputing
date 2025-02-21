package org.MDGA;

//TIP 要<b>运行</b>代码，请按 <shortcut actionId="Run"/> 或
// 点击装订区域中的 <icon src="AllIcons.Actions.Execute"/> 图标。
public class Main {
    public static void main(String[] args) throws Exception {
        /*
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092") // 指定Kafka节点的地址和端口
                .setGroupId("org.example")                               // 指定消费者组的id
                .setTopics("topic_1")                                    // 指定消费的Topic
                .setValueOnlyDeserializer(new SimpleStringSchema())      // 指定反序列化器(反序列化Value)
                .setStartingOffsets(OffsetsInitializer.earliest())       // 指定Flink消费Kafka的策略
                .build();

        // 3个参数: fileSource实例, Watermark, source名称
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource")
                .print();

        env.execute();
        */
    }
}