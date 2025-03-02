package stream.source;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import stream.BaseDataStream;
import stream.DataStream;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

/**
 * Kafka数据源实现
 * @param <T> 数据类型
 */
public class KafkaSource<T> implements Source<T> {
    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final Function<String, T> valueDeserializer;
    private final List<T> buffer;
    private final int batchSize;

    /**
     * 创建Kafka数据源
     *
     * @param bootstrapServers Kafka服务器地址
     * @param topic 订阅的主题
     * @param groupId 消费者组ID
     * @param valueDeserializer 值反序列化函数
     * @param batchSize 批处理大小
     */
    public KafkaSource(String bootstrapServers, String topic, String groupId,
                      Function<String, T> valueDeserializer, int batchSize) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // 添加分布式和容错配置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);      // 禁用自动提交
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSize);   // 限制单次拉取记录数
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);     // 会话超时时间
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);  // 心跳间隔
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);  // 最大轮询间隔

        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.valueDeserializer = valueDeserializer;
        this.buffer = new ArrayList<>();
        this.batchSize = batchSize;

        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public DataStream<T> getDataStream() {
        buffer.clear();
        try {
            while (buffer.size() < batchSize) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        T value = valueDeserializer.apply(record.value());
                        buffer.add(value);
                        if (buffer.size() >= batchSize) {
                            break;
                        }
                    } catch (Exception e) {
                        System.err.println(String.format("反序列化数据失败 - Partition: %d, Offset: %d, Error: %s",
                            record.partition(), record.offset(), e.getMessage()));
                        // 继续处理下一条记录
                    }
                }
                // 手动提交偏移量
                consumer.commitSync();
            }
            return new BaseDataStream<>(new ArrayList<>(buffer));
        } catch (Exception e) {
            System.err.println(String.format("从Kafka获取数据失败 - Topic: %s, Error: %s", topic, e.getMessage()));
            throw new RuntimeException("从Kafka获取数据失败", e);
        }
    }

    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
}