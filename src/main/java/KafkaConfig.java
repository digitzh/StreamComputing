
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

/**
 * Kafka 客户端配置
 */
public class KafkaConfig {
    public static Properties getConsumerConfig() {
        Properties props = new Properties();
        // 修改为本地开发环境的Kafka服务器地址
        String IP_address = "localhost:9092";
        props.put("bootstrap.servers", IP_address);
        props.put("group.id", "streaming-job");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    public static Properties getProducerConfig() {
        Properties props = new Properties();
        // 修改为本地开发环境的Kafka服务器地址
        String IP_address = "localhost:9092";
        props.put("bootstrap.servers", IP_address);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("max.in.flight.requests.per.connection", 1);
        return props;
    }
}
