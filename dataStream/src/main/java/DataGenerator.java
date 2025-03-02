import java.util.function.Supplier;

/**
 * 模拟数据生成器（替代手动emit测试数据）
 * 使用示例：
 * DataGenerator<String> generator = new DataGenerator<>(
 *     sourceStream,
 *     () -> "user," + System.currentTimeMillis(), // 自定义数据生成逻辑
 *     1000 // 间隔毫秒
 * );
 * new Thread(generator).start();
 */
public class DataGenerator<T> implements Runnable {
    private final DataStream<T> stream;
    private final Supplier<T> dataSupplier;
    private final long intervalMillis;
    private volatile boolean running = true;

    public DataGenerator(DataStream<T> stream, Supplier<T> dataSupplier, long intervalMillis) {
        this.stream = stream;
        this.dataSupplier = dataSupplier;
        this.intervalMillis = intervalMillis;
    }

    @Override
    public void run() {
        try {
            while (running) {
                T data = dataSupplier.get();
                stream.emit(data);
                Thread.sleep(intervalMillis);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        running = false;
    }
}
