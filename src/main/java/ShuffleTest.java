import java.util.concurrent.TimeUnit;

public class ShuffleTest {
    public static void main(String[] args) throws InterruptedException {
        // 创建数据流
        SetParallelism setParallelism = new SetParallelism();
        DataStream<String> sourceStream = new DataStream<String>(setParallelism.new StreamOperator<String>() {
            @Override
            protected Worker<String> createWorker(int workerId) {
                return new SetParallelism.StreamOperator<String>.Worker<String>(workerId) {
                    @Override
                    protected void processRecord(StreamRecord<String> record) {
                        // 处理记录的逻辑
                    }
                };
            }
        });
        DataStream<String> shuffledStream = new DataStream<String>(setParallelism.new StreamOperator<String>() {
            @Override
            protected Worker<String> createWorker(int workerId) {
                return new SetParallelism.StreamOperator<String>.Worker<String>(workerId) {
                    @Override
                    protected void processRecord(StreamRecord<String> record) {
                        // 处理记录的逻辑
                    }
                };
            }
        });
        DataStream<String> outputStream = new DataStream<String>(setParallelism.new StreamOperator<String>() {
            @Override
            protected Worker<String> createWorker(int workerId) {
                return new SetParallelism.StreamOperator<String>.Worker<String>(workerId) {
                    @Override
                    protected void processRecord(StreamRecord<String> record) {
                        // 处理记录的逻辑
                    }
                };
            }
        });

        // 创建数据生成器
        DataGenerator<String> generator = new DataGenerator<>(
            sourceStream,
            () -> {
                String[] words = {"apple", "banana", "orange", "grape"};
                int index = (int) (Math.random() * words.length);
                return words[index];
            },
            100 // 每100ms生成一条数据
        );

        // 创建Shuffle算子
        ShuffleOperator<String> shuffleOperator = new ShuffleOperator<>(
            sourceStream,
            shuffledStream,
            "shuffle-input-topic",
            "shuffle-output-topic"
        );
        shuffleOperator.setParallelism(3); // 设置并行度为3

        // 创建Sink算子用于验证结果
        KafkaSink kafkaSink = new KafkaSink(
            shuffledStream,
                KafkaConfig.IP_PORT,
            "test-output-topic"
        );

        // 启动所有组件
        Thread generatorThread = new Thread(generator);
        Thread sinkThread = new Thread(kafkaSink);

        generatorThread.start();
        shuffleOperator.start();
        sinkThread.start();

        // 运行一段时间后停止
        TimeUnit.SECONDS.sleep(30);

        // 停止所有组件
        generator.stop();
        shuffleOperator.stop();
        kafkaSink.stop();

        // 等待线程结束
        generatorThread.join();
        sinkThread.join();

        System.out.println("Shuffle测试完成");
    }
}