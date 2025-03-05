import java.util.concurrent.TimeUnit;

public class TestKeyByReduce {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("开始测试KeyBy和Reduce操作的正确性...");

        // 创建数据流
        DataStream<String> sourceStream = new DataStream<>();
        KeyedDataStream<String, String> keyedStream = new KeyedDataStream<>();
        DataStream<String> reduceStream = new DataStream<>();

        // 创建数据生成器，模拟输入数据
        DataGenerator<String> generator = new DataGenerator<>(
                sourceStream,
                () -> {
                    String[] users = {"user1", "user2", "user3"};
                    String[] actions = {"login", "logout", "click"};
                    String user = users[(int)(Math.random() * users.length)];
                    String action = actions[(int)(Math.random() * actions.length)];
                    return user + "," + action;
                },
                1000 // 每1秒生成一条数据
        );

        // 创建KeyBy算子，使用用户ID作为key
        System.out.println("\n=== 开始KeyBy并行度测试 ===");
        System.out.println("初始设置并行度为2，验证数据是否按key正确分配到不同的并行实例");
        KeyByOperator<String, String> keyByOperator = new KeyByOperator<>(
                sourceStream,
                record -> record.split(",")[0], // 提取用户ID作为key
                2 // 初始设置并行度为2
        );

        // 创建Reduce算子，将同一用户的行为连接起来
        ReduceOperator<String, String> reduceOperator = new ReduceOperator<>(
                keyByOperator.getKeyedStreams(),
                reduceStream,
                (acc, curr) -> acc + "|" + curr
        );

        // 创建KafkaSink，将结果写入Kafka
        KafkaSink kafkaSink = new KafkaSink(
                reduceStream,
                "localhost:9092",
                "keyby-reduce-test-topic"
        );

        // 启动所有线程
        Thread generatorThread = new Thread(generator);
        Thread keyByThread = new Thread(keyByOperator);
        Thread reduceThread = new Thread(reduceOperator);
        Thread sinkThread = new Thread(kafkaSink);

        generatorThread.start();
        keyByThread.start();
        reduceThread.start();
        sinkThread.start();

        // 运行10秒后调整并行度
        TimeUnit.SECONDS.sleep(10);
        System.out.println("\n=== 调整KeyBy并行度 ===");
        System.out.println("将并行度从2调整为3，验证数据重新分配情况");
        keyByOperator.setParallelism(3);

        // 继续运行20秒
        TimeUnit.SECONDS.sleep(20);

        // 运行30秒后停止
        TimeUnit.SECONDS.sleep(30);

        // 停止所有组件
        generator.stop();
        keyByOperator.stop();
        reduceOperator.stop();
        kafkaSink.stop();

        // 等待线程结束
        generatorThread.join();
        keyByThread.join();
        reduceThread.join();
        sinkThread.join();

        System.out.println("测试完成。");
    }
}