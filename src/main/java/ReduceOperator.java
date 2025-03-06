import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.concurrent.TimeUnit;

/**
 * 基于KeyedStream的Reduce算子
 * @param <K> Key类型
 * @param <T> 数据类型
 */
public class ReduceOperator<K, T> extends StreamOperator {
    private final KeyedDataStream<K, T> inputStream;
    private final DataStream<T> outputStream;
    private final BiFunction<T, T, T> reducer;
    private final ConcurrentHashMap<K, T> accumulator = new ConcurrentHashMap<>();

    public ReduceOperator(KeyedDataStream<K, T> inputStream,
                          DataStream<T> outputStream,
                          BiFunction<T, T, T> reducer,
                          int parallelism) { // 添加并行度参数
        super(parallelism); // 调用父类构造器
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.reducer = reducer;
    }

    @Override
    protected void rebalanceWorkers() {
        // 清空累积器
        accumulator.clear();
    }

    @Override
    public void run() {
        // 启动并行工作线程（使用父类的executorService）
        for (int i = 0; i < parallelism; i++) {
            final int workerId = i;
            executorService.submit(() -> processRecords(workerId));
        }
    }

    private void processRecords(int workerId) {
        try {
            while (isRunning || !inputStream.getKeyedStreams().isEmpty()) {
                inputStream.getKeyedStreams().forEach((key, stream) -> {
                    // 根据key的hash分配工作线程
                    if (Math.abs(key.hashCode() % parallelism) == workerId) {
                        try {
                            T value = stream.poll(100, TimeUnit.MILLISECONDS);
                            if (value != null) {
                                // 验证相同key的数据是否来自同一个工作线程
                                String valueStr = value.toString();
                                if (valueStr.contains("Worker")) {
                                    String workerIdStr = valueStr.substring(valueStr.indexOf("Worker") + 6, valueStr.indexOf("]"));
                                    System.out.println(String.format("[Reduce-Verification] Key %s received data from Worker-%s", key, workerIdStr));
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
}
