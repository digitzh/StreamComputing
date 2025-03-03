import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * 将数据写入Kafka的Sink算子
 */
public class KafkaSink implements Runnable {
    private final DataStream<String> dataStream;
    private final String bootstrapServers;
    private final String topic;
    private volatile boolean isRunning = true;
    private KafkaProducer<String, String> producer;

    public KafkaSink(DataStream<String> dataStream, String bootstrapServers, String topic) {
        this.dataStream = dataStream;
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                String record = dataStream.poll();
                if (record != null) {
                    producer.send(new ProducerRecord<>(topic, record));
                    System.out.println("[KafkaSink] Sent record: " + record);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public void stop() {
        isRunning = false;
    }
}
