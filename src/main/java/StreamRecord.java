public class StreamRecord<T> {
    private final Object key;
    private final T value;

    public StreamRecord(Object key, T value) {
        this.key = key;
        this.value = value;
    }

    public Object getKey() {
        return key;
    }

    public T getValue() {
        return value;
    }
}