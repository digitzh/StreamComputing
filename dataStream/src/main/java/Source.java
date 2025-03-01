
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * 从 Kafka 读取数据的 Source 算子
 */
public class Source implements Runnable {
    private final DataStream<String> dataStream;
    private volatile boolean isRunning = true;
    private final KafkaConsumer<String, String> consumer;

    public Source(DataStream<String> dataStream, String topic) {
        this.dataStream = dataStream;
        Properties props = KafkaConfig.getConsumerConfig();
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    dataStream.emit(record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

    public void stop() {
        isRunning = false;
    }
}