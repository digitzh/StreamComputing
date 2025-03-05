/**
 * 测试基本的Operator（Source, Map, KeyBy, Reduce & Sink）
*/

public class TestBasicOperator {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("工作目录: " + System.getProperty("user.dir"));

        // 原始数据流（String），来自 Kafka 或 DataGenerator
        DataStream<String> sourceStream = new DataStream<>();
        DataStream<WordCountEvent> mappedStream = new DataStream<>();

        // 1. source算子（从Kafka读取或使用 DataGenerator 模拟输入）
        Source source = new Source(sourceStream, "input-topic");

        // 2. Map算子：解析输入并转换
        MapOperator<String, WordCountEvent> mapOperator = new MapOperator<>(
                sourceStream,
                mappedStream,
                record -> {
                    String[] parts = record.split(",");
                    return new WordCountEvent(parts[0], Integer.parseInt(parts[1]), System.currentTimeMillis());
                },
                1
        );

        // 3. 添加KeyBy和Reduce算子
        // KeyBy算子按单词分组
        KeyByOperator<WordCountEvent, String> keyByOperator = new KeyByOperator<>(
                mappedStream,
                event -> event.word,  // 按单词分组
                1  // 基本测试，并行度暂时设为1（可以调整）
        );

        // Reduce算子累加计数
        DataStream<WordCountEvent> reducedStreamWC = new DataStream<>();
        ReduceOperator<String, WordCountEvent> reduceOperator = new ReduceOperator<>(
                keyByOperator.getKeyedStreams(),
                reducedStreamWC,
                (current, newEvent) -> new WordCountEvent(
                        current.word,
                        current.count + newEvent.count,
                        System.currentTimeMillis()
                )
        );

        // 3. Reduce后的序列化Map
        DataStream<String> serializedStream = new DataStream<>();
        MapOperator<WordCountEvent, String> serializeOperator = new MapOperator<>(
                reducedStreamWC,
                serializedStream,
                event -> event.word + ":" + event.count,  // 转换为字符串格式
                1
        );

        // 4. Sink算子：将聚合结果写入文件
        //Sink sink = new Sink(reducedKeyStream, "output.txt");
        // 4. Sink算子：将聚合结果写入Kafka
        KafkaSink kafkaSink = new KafkaSink(
                serializedStream,
                KafkaConfig.IP_PORT,  // Kafka broker地址
                "output-topic"     // 目标topic
        );

        // 启动各个算子线程
        Thread sourceThread = new Thread(source);
        sourceThread.start();

        Thread mapThread = new Thread(mapOperator);
        mapThread.start();
        Thread serializeThread = new Thread(serializeOperator);
        serializeThread.start();

        // 添加KeyBy和Reduce线程
        Thread keyByThread = new Thread(keyByOperator);
        keyByThread.start();
        Thread reduceThread = new Thread(reduceOperator);
        reduceThread.start();

        // (可选)输出到文件
//        Thread sinkThread = new Thread(sink);
        // 输出到Kafka
        Thread sinkThread = new Thread(kafkaSink);
        sinkThread.start();

        // 运行一段时间后停止
        Thread.sleep(60_000); // 运行 60 秒后停止

        source.stop();
        mapOperator.stop();
        serializeOperator.stop();
//        sinkThread.stop();
        kafkaSink.stop();
        keyByOperator.stop();
        reduceOperator.stop();

        // 等待线程结束
        keyByThread.join();
        reduceThread.join();
        sourceThread.join();
        serializeThread.join();
        sinkThread.join();
    }
}
