import java.util.function.Function;
import java.util.concurrent.TimeUnit;

/**
 * 键值分组算子
 */
public class KeyByOperator<T, K> implements Runnable {
    private final DataStream<T> inputStream;
    private final KeyedDataStream<K, T> outputStreams;
    private final Function<T, K> keySelector;
    private volatile boolean isRunning = true;
    private final int parallelism;

    public KeyByOperator(DataStream<T> inputStream,
                        Function<T, K> keySelector,
                        int parallelism) {
        this.inputStream = inputStream;
        this.keySelector = keySelector;
        this.parallelism = parallelism;
        this.outputStreams = new KeyedDataStream<>();
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                T record = inputStream.poll(100, TimeUnit.MILLISECONDS); // 添加超时以避免无限阻塞
                if (record != null) {
                    K key = keySelector.apply(record);
                    System.out.println("[KeyBy]Received record: " + record + ", key: " + key);
                    outputStreams.emit(key, record);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // 清理资源
            outputStreams.clear();
        }
    }

    public void stop() {
        isRunning = false;
    }

    public KeyedDataStream<K, T> getKeyedStreams() {
        return outputStreams;
    }
}
