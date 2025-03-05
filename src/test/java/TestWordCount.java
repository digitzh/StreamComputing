public class TestWordCount {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("工作目录: " + System.getProperty("user.dir"));
        // 原始数据流（String），来自 Kafka 或 DataGenerator
        DataStream<String> sourceStream = new DataStream<>();

        // 用于解析后的 WordCountEvent 流
        DataStream<WordCountEvent> parsedStream = new DataStream<>();
        // 窗口聚合结果流（String 格式：window_trigger_time,word,total_count）
        DataStream<String> windowedStream = new DataStream<>();

        // 1. Source算子（从Kafka读取或使用 DataGenerator 模拟输入）
        Source source = new Source(sourceStream, "input-topic");

        // 2. Map算子：解析输入并转换为 WordCountEvent
        MapOperator<String, WordCountEvent> parseOperator = new MapOperator<>(
                sourceStream,
                parsedStream,
                s -> {
                    // 输入格式："word,count"，例如 "APPLE,1"
                    String[] parts = s.split(",");
                    String word = parts[0].toLowerCase();  // 大小写不敏感
                    int count = Integer.parseInt(parts[1]);
                    return new WordCountEvent(word, count, System.currentTimeMillis());
                },
                1
        );

        // 3. Window算子：滚动窗口聚合
        // 10秒 = 10 * 1000 毫秒
        WindowOperator windowOperator = new WindowOperator(parsedStream, windowedStream, 10 * 1000);

        // 4. Sink算子：将聚合结果写入文件
        //Sink sink = new Sink(windowedStream, "output.txt");
        // 4. Sink算子：将聚合结果写入Kafka
        KafkaSink kafkaSink = new KafkaSink(
                windowedStream,
                KafkaConfig.IP_PORT,  // Kafka broker地址
                "output-topic"     // 目标topic
        );

        // 启动各个算子线程
        Thread sourceThread = new Thread(source);
        sourceThread.start();

        Thread parseThread = new Thread(parseOperator);
        parseThread.start();

        Thread windowThread = new Thread(windowOperator);
        windowThread.start();

        // 输出到文件
//        Thread sinkThread = new Thread(sink);
//        sinkThread.start();
        // 输出到Kafka
        Thread sinkThread = new Thread(kafkaSink);
        sinkThread.start();

        // 模拟数据生成（如果不用 Kafka 输入，可用 DataGenerator 模拟数据）
        DataGenerator<String> generator = new DataGenerator<>(
                sourceStream,
                () -> {
                    // 示例随机生成 "apple,1"、"banana,1"、"orange,1" 等Word数据
                    String[] words = {"apple", "banana", "orange", "pear", "watermelon", "strawberry"};
                    // 为便于测试，可以固定生成顺序或随机生成
                    int index = (int) (Math.random() * words.length);
                    return words[index] + ",1";
                },
                250  // 每250ms生成一条数据
        );
        Thread generatorThread = new Thread(generator);
        generatorThread.start();

        // 运行一段时间后停止
        Thread.sleep(30_000); // 运行 30 秒后停止
        source.stop();
        parseOperator.stop();
        windowOperator.stop();
//        sink.stop();
        kafkaSink.stop();
        generator.stop();

        // 等待线程结束
        sourceThread.join();
        parseThread.join();
        windowThread.join();
        sinkThread.join();
        generatorThread.join();
    }
}
