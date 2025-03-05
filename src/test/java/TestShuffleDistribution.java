import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;

public class TestShuffleDistribution {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("开始测试Shuffle的负载均衡性和顺序性...\n");

        // 创建数据流
        DataStream<String> sourceStream = new DataStream<>();
        DataStream<String> shuffledStream = new DataStream<>();

        // 创建数据生成器，模拟带key的输入数据
        DataGenerator<String> generator = new DataGenerator<>(
            sourceStream,
            () -> {
                // 生成10个不同的key，每个key重复多次以验证顺序性
                String[] keys = {"key1", "key2", "key3", "key4", "key5", 
                                "key6", "key7", "key8", "key9", "key10"};
                String[] values = {"value1", "value2", "value3", "value4", "value5"};
                String key = keys[(int)(Math.random() * keys.length)];
                String value = values[(int)(Math.random() * values.length)];
                return key + "," + value;
            },
            100 // 每100ms生成一条数据
        );

        // 创建Shuffle算子，设置4个分区
        ShuffleOperator<String> shuffleOperator = new ShuffleOperator<>(
            sourceStream,
            shuffledStream,
            "shuffle-distribution-input",
            "shuffle-distribution-output"
        );
        shuffleOperator.setParallelism(4); // 设置4个分区

        // 创建统计信息收集器
        Map<Integer, AtomicInteger> partitionCounts = new ConcurrentHashMap<>();
        Map<String, Integer> keyPartitionMapping = new ConcurrentHashMap<>();

        // 创建KafkaSink，将结果写入Kafka并收集统计信息
        KafkaSink kafkaSink = new KafkaSink(
            shuffledStream,
                KafkaConfig.IP_PORT,
            "shuffle-distribution-test-topic"
        ) {
            protected void processRecord(String record, int partition) {
                // 更新分区计数
                partitionCounts.computeIfAbsent(partition, k -> new AtomicInteger()).incrementAndGet();
                
                // 检查key的分区分配
                String key = record.split(",")[0];
                Integer previousPartition = keyPartitionMapping.get(key);
                if (previousPartition == null) {
                    keyPartitionMapping.put(key, partition);
                } else if (previousPartition != partition) {
                    System.out.println("警告：键 " + key + " 的数据被分配到不同分区：之前=" + 
                                     previousPartition + ", 当前=" + partition);
                }
                System.out.println("[KafkaSink] Sent record: " + record + " to partition: " + partition);
            }
        };

        // 启动所有组件
        Thread generatorThread = new Thread(generator);
        Thread sinkThread = new Thread(kafkaSink);

        generatorThread.start();
        shuffleOperator.start();
        sinkThread.start();

        // 每5秒打印一次统计信息
        for (int i = 0; i < 6; i++) {
            TimeUnit.SECONDS.sleep(5);
            System.out.println("\n=== 统计信息 ===\n分区数据分布：");
            partitionCounts.forEach((partition, count) -> 
                System.out.println("分区 " + partition + ": " + count.get() + " 条记录"));
            
            System.out.println("\nKey分区映射：");
            keyPartitionMapping.forEach((key, partition) -> 
                System.out.println("Key '" + key + "' -> 分区 " + partition));
        }

        // 停止所有组件
        generator.stop();
        shuffleOperator.stop();
        kafkaSink.stop();

        // 等待线程结束
        generatorThread.join();
        sinkThread.join();

        // 输出最终统计结果
        System.out.println("\n=== 最终统计结果 ===\n分区数据分布：");
        partitionCounts.forEach((partition, count) -> 
            System.out.println("分区 " + partition + ": " + count.get() + " 条记录"));

        // 计算数据分布的标准差，评估负载均衡性
        double mean = partitionCounts.values().stream()
            .mapToInt(AtomicInteger::get)
            .average()
            .orElse(0.0);
        
        double variance = partitionCounts.values().stream()
            .mapToDouble(count -> Math.pow(count.get() - mean, 2))
            .average()
            .orElse(0.0);
        
        double stdDev = Math.sqrt(variance);
        System.out.println("\n负载均衡性分析：");
        System.out.println("平均每个分区记录数：" + mean);
        System.out.println("标准差：" + stdDev);
        System.out.println("变异系数：" + (stdDev / mean));

        System.out.println("\n测试完成。");
    }
}