
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;

/**
 * Kafka 客户端配置
 */
public class KafkaConfig {
    public static Properties getConsumerConfig() {
        Properties props = new Properties();
        // 修改为你的 Kafka 服务器地址
        String IP_address = "192.168.233.129:9092";
        props.put("bootstrap.servers", IP_address);
        props.put("group.id", "streaming-job");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        return props;
    }
}
