package stream.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Kafka输出目标实现
 * @param <T> 数据类型
 */
public class KafkaSink<T> implements Sink<T> {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Function<T, String> valueSerializer;

    /**
     * 创建Kafka输出目标
     *
     * @param bootstrapServers Kafka服务器地址
     * @param topic 目标主题
     * @param valueSerializer 值序列化函数
     */
    public KafkaSink(String bootstrapServers, String topic, Function<T, String> valueSerializer) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 添加容错和可靠性配置
        props.put(ProducerConfig.ACKS_CONFIG, "all");                     // 等待所有副本确认
        props.put(ProducerConfig.RETRIES_CONFIG, 3);                      // 重试次数
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);         // 重试间隔
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);       // 启用幂等性
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5); // 限制进行中的请求数

        this.producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void write(T data) {
        try {
            String value = valueSerializer.apply(data);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    // 记录详细的错误信息
                    System.err.println(String.format("发送消息失败 - Topic: %s, Error: %s, Cause: %s", 
                        topic, exception.getMessage(), exception.getCause()));
                    // TODO: 可以在这里实现死信队列逻辑
                }
            }).get(5, TimeUnit.SECONDS); // 添加超时限制
        } catch (Exception e) {
            System.err.println(String.format("序列化或发送数据失败 - Error: %s, Cause: %s", 
                e.getMessage(), e.getCause()));
            // TODO: 可以在这里实现重试逻辑或将失败消息写入死信队列
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close(Duration.ofSeconds(5));
            } catch (Exception e) {
                System.err.println("关闭生产者失败: " + e.getMessage());
            }
        }
    }
}