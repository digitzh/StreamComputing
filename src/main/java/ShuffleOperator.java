import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.Set;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

public class ShuffleOperator<T> {
    private final SetParallelism setParallelism = new SetParallelism();
    private final SetParallelism.StreamOperator<T> operator;
    private final String inputTopic;
    private final String outputTopic;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running;
    private final DataStream<T> outputStream;
    private final DataStream<T> inputStream;
    private final Map<Integer, AtomicLong> partitionCounts = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> keyPartitionMapping = new ConcurrentHashMap<>();
    private final ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();

    public ShuffleOperator(DataStream<T> inputStream, DataStream<T> outputStream,
                           String inputTopic, String outputTopic) {
        this.operator = setParallelism.new StreamOperator<T>() {
            @Override
            protected Worker<T> createWorker(int workerId) {
                return new ShuffleWorker(workerId);
            }
        };
        this.inputStream = inputStream;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.outputStream = outputStream;
        this.running = new AtomicBoolean(true);

        // 配置生产者
        Properties producerProps = KafkaConfig.getConsumerConfig();
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        producerProps.put("acks", "all");
        producerProps.put("retries", 3);
        producerProps.put("max.in.flight.requests.per.connection", 1);
        this.producer = new KafkaProducer<>(producerProps);

        // 配置消费者
        Properties consumerProps = KafkaConfig.getConsumerConfig();
        consumerProps.put("group.id", "shuffle-group");
        this.consumer = new KafkaConsumer<>(consumerProps);
    }

    private void updateStats(String key, int partition) {
        partitionCounts.computeIfAbsent(partition, k -> new AtomicLong()).incrementAndGet();
        keyPartitionMapping.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(partition);
    }

    private void printStats() {
        System.out.println("\n=== Shuffle Statistics ===");

        // 打印分区负载统计
        System.out.println("\nPartition Distribution:");
        long[] counts = partitionCounts.values().stream().mapToLong(AtomicLong::get).toArray();
        double mean = Arrays.stream(counts).average().orElse(0.0);
        double variance = Arrays.stream(counts)
                .mapToDouble(count -> Math.pow(count - mean, 2))
                .average().orElse(0.0);
        double stdDev = Math.sqrt(variance);
        double cv = mean != 0 ? stdDev / mean : 0;

        partitionCounts.forEach((partition, count) ->
                System.out.printf("Partition %d: %d records\n", partition, count.get()));
        System.out.printf("\nDistribution Metrics:\nMean: %.2f\nStandard Deviation: %.2f\nCoefficient of Variation: %.2f\n",
                mean, stdDev, cv);

        // 打印key路由统计
        System.out.println("\nKey Routing:");
        keyPartitionMapping.forEach((key, partitions) ->
                System.out.printf("Key %s -> Partitions: %s\n", key, partitions));
    }

    private class ShuffleWorker extends SetParallelism.StreamOperator<T>.Worker<T> {
        public ShuffleWorker(int workerId) {
            operator.super(workerId);
        }

        @Override
        protected void processRecord(StreamRecord<T> record) {
            // 将数据发送到Kafka以进行Shuffle
            String key = String.valueOf(record.getKey().hashCode() % operator.getParallelism());
            String value = record.getValue().toString();
            producer.send(new ProducerRecord<>(outputTopic, key, value));
        }
    }

    @SuppressWarnings("unchecked")
    public void start() {
        // 启动统计信息定期打印任务
        statsExecutor.scheduleAtFixedRate(this::printStats, 10, 10, TimeUnit.SECONDS);

        // 启动生产者线程，从inputStream读取数据并发送到Kafka
        Thread producerThread = new Thread(() -> {
            while (running.get()) {
                try {
                    T data = inputStream.poll(100, TimeUnit.MILLISECONDS);
                    if (data != null) {
                        String key = String.valueOf(data.hashCode() % operator.getParallelism());
                        producer.send(new ProducerRecord<>(inputTopic, key, data.toString()));
                        updateStats(key, Math.abs(key.hashCode() % operator.getParallelism()));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        producerThread.start();

        // 订阅输入主题
        consumer.subscribe(Collections.singletonList(inputTopic));

        // 启动消费线程
        Thread consumerThread = new Thread(() -> {
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // 将数据发送到输出流
                    T value = (T) record.value();
                    outputStream.emit(value);
                }
            }
            consumer.close();
        });
        consumerThread.start();
    }

    public void stop() {
        running.set(false);
        try {
            // 打印最终统计信息
            System.out.println("\n=== Final Shuffle Statistics ===");
            printStats();

            // 关闭统计执行器
            statsExecutor.shutdown();
            statsExecutor.awaitTermination(5, TimeUnit.SECONDS);

            // 等待一段时间确保消费者线程有机会退出循环
            Thread.sleep(1000);

            // 先关闭生产者
            producer.flush();
            producer.close(Duration.ofSeconds(5));

            // 等待一段时间确保所有消息都被消费
            Thread.sleep(2000);

            // 在主线程中安全地关闭消费者
            synchronized (consumer) {
                consumer.close(Duration.ofSeconds(5));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setParallelism(int parallelism) {
        operator.setParallelism(parallelism);
    }
}