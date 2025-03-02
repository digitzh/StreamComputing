import java.util.concurrent.ConcurrentHashMap; /**
 * 带键值分组的扩展数据流
 */
public class KeyedDataStream<K, V> {
    private final ConcurrentHashMap<K, DataStream<V>> streams = new ConcurrentHashMap<>();

    public void emit(K key, V value) {
        streams.computeIfAbsent(key, k -> new DataStream<>()).emit(value);
    }

    public DataStream<V> getStream(K key) {
        return streams.getOrDefault(key, new DataStream<>());
    }

    public KeyedDataStream<K, V> getKeyedStreams() {
        return this;
    }
    public void forEach(java.util.function.BiConsumer<K, DataStream<V>> action) {
        streams.forEach(action);
    }
}
