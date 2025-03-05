import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.concurrent.TimeUnit;

/**
 * 基于KeyedStream的Reduce算子
 * @param <K> Key类型
 * @param <T> 数据类型
 */
public class ReduceOperator<K, T> implements Runnable {
    private final KeyedDataStream<K, T> inputStream;
    private final DataStream<T> outputStream;
    private final BiFunction<T, T, T> reducer;
    private final ConcurrentHashMap<K, T> accumulator = new ConcurrentHashMap<>();
    private volatile boolean isRunning = true;

    public ReduceOperator(KeyedDataStream<K, T> inputStream,
                          DataStream<T> outputStream,
                          BiFunction<T, T, T> reducer) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.reducer = reducer;
    }

    @Override
    public void run() {
        try {
            while (isRunning || !inputStream.getKeyedStreams().isEmpty()) {
                // 遍历所有key的stream
                inputStream.getKeyedStreams().forEach((key, stream) -> {
                    try {
                        T value = stream.poll(100, TimeUnit.MILLISECONDS); // 添加超时以避免无限阻塞
                        if (value != null) {
                            // 验证相同key的数据是否来自同一个工作线程
                            String valueStr = value.toString();
                            if (valueStr.contains("Worker")) {
                                String workerId = valueStr.substring(valueStr.indexOf("Worker") + 6, valueStr.indexOf("]"));
                                System.out.println(String.format("[Reduce-Verification] Key %s received data from Worker-%s", key, workerId));
                            }
                            T current = accumulator.getOrDefault(key, null);
                            T reduced = (current == null) ? value : reducer.apply(current, value);
                            accumulator.put(key, reduced);
                            System.out.println("[Reduce]Reduced value for key " + key + ": " + reduced);
                            outputStream.emit(reduced);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        } finally {
            // 在停止前输出最终的累积结果
            accumulator.forEach((key, value) -> {
                System.out.println("[Reduce]Final value for key " + key + ": " + value);
                outputStream.emit(value);
            });
            accumulator.clear();
        }
    }

    public void stop() {
        isRunning = false;
    }
}
