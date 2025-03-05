import java.util.function.Function;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 键值分组算子
 */
public class KeyByOperator<T, K> extends StreamOperator {
    private final DataStream<T> inputStream;
    private final KeyedDataStream<K, T> outputStreams;
    private final Function<T, K> keySelector;
    private final Map<Integer, AtomicInteger> workerStats;

    public KeyByOperator(DataStream<T> inputStream,
                         Function<T, K> keySelector,
                         int parallelism) {
        super(parallelism);
        this.inputStream = inputStream;
        this.keySelector = keySelector;
        this.outputStreams = new KeyedDataStream<>();
        this.workerStats = new ConcurrentHashMap<>();
        initializeWorkerStats();
    }

    private void initializeWorkerStats() {
        workerStats.clear();
        for (int i = 0; i < parallelism; i++) {
            workerStats.put(i, new AtomicInteger(0));
        }
    }

    @Override
    protected void rebalanceWorkers() {
        // 清理并重新初始化工作线程统计信息
        initializeWorkerStats();

        // 启动新的工作线程
        for (int i = 0; i < parallelism; i++) {
            final int workerId = i;
            executorService.submit(() -> processRecords(workerId));
        }
    }

    @Override
    public void run() {
        // 启动并行工作线程
        for (int i = 0; i < parallelism; i++) {
            final int workerId = i;
            executorService.submit(() -> processRecords(workerId));
        }

        try {
            while (isRunning) {
                TimeUnit.SECONDS.sleep(5);
                printWorkerStats();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executorService.shutdownNow();
            outputStreams.clear();
        }
    }

    private void processRecords(int workerId) {
        try {
            while (isRunning) {
                T record = inputStream.poll(100, TimeUnit.MILLISECONDS);
                if (record != null) {
                    K key = keySelector.apply(record);
                    int targetWorker = Math.abs(key.hashCode() % parallelism);
                    if (targetWorker == workerId) {
                        System.out.println(String.format("[KeyBy-Worker%d] Processing record: %s, key: %s, targetWorker: %d",
                                workerId, record, key, targetWorker));
                        outputStreams.emit(key, record);
                        workerStats.get(workerId).incrementAndGet();
                        // 打印key分配信息
                        System.out.println(String.format("[KeyBy-Distribution] Key: %s is consistently processed by Worker-%d (hash: %d)",
                                key, workerId, key.hashCode()));
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void printWorkerStats() {
        System.out.println("\n=== KeyBy Worker Statistics ===");
        for (int i = 0; i < parallelism; i++) {
            System.out.println(String.format("Worker-%d: processed %d records", i, workerStats.get(i).get()));
        }
        System.out.println("============================\n");
    }

    public void stop() {
        isRunning = false;
        executorService.shutdownNow();
    }

    public KeyedDataStream<K, T> getKeyedStreams() {
        return outputStreams;
    }
}
