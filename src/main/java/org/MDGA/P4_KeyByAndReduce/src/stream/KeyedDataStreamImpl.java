package stream;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.logging.Logger;

/**
 * KeyedDataStream接口的具体实现类
 * @param <T> 数据类型
 * @param <K> key的类型
 */
public class KeyedDataStreamImpl<T, K> implements KeyedDataStream<T, K> {
    private static final Logger logger = Logger.getLogger(KeyedDataStreamImpl.class.getName());
    private final BaseDataStream<T> parentStream;
    private final Function<T, K> keyExtractor;
    private final ConcurrentHashMap<K, List<T>> groupedData;
    private final ExecutorService executorService;

    public KeyedDataStreamImpl(BaseDataStream<T> parentStream, Function<T, K> keyExtractor) {
        this.parentStream = parentStream;
        this.keyExtractor = keyExtractor;
        this.groupedData = new ConcurrentHashMap<>(parentStream.getData().stream()
                .collect(Collectors.groupingBy(keyExtractor)));
        this.executorService = Executors.newFixedThreadPool(parentStream.getParallelism());
    }

    @Override
    public DataStream<T> reduce(BinaryOperator<T> reducer) {
        try {
            List<Future<T>> futures = new ArrayList<>();
            List<T> reducedData = new ArrayList<>();

            // 并行处理每个分组
            for (List<T> group : groupedData.values()) {
                if (!group.isEmpty()) {
                    Future<T> future = executorService.submit(() -> 
                        group.stream().reduce(reducer).orElse(null));
                    futures.add(future);
                }
            }

            // 收集并行处理的结果
            for (Future<T> future : futures) {
                try {
                    T result = future.get(parentStream.getWindowSize(), TimeUnit.MILLISECONDS);
                    if (result != null) {
                        reducedData.add(result);
                    }
                } catch (TimeoutException e) {
                    logger.warning("分组归约操作超时: " + e.getMessage());
                } catch (Exception e) {
                    logger.severe("分组归约操作失败: " + e.getMessage());
                }
            }

        return new BaseDataStream<>(reducedData)
                .setParallelism(parentStream.getParallelism())
                .withWindow(parentStream.getWindowSize());
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(parentStream.getWindowSize(), TimeUnit.MILLISECONDS)) {
                    logger.warning("线程池未能在指定时间内完全关闭");
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.severe("关闭线程池时被中断: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public K getKey() {
        if (groupedData.isEmpty()) {
            throw new IllegalStateException("没有可用的key，数据流为空");
        }
        return groupedData.keySet().iterator().next();
    }

    @Override
    public <NK> KeyedDataStream<T, NK> keyBy(Function<T, NK> newKeyExtractor) {
        return new KeyedDataStreamImpl<>(parentStream, newKeyExtractor);
    }

    @Override
    public DataStream<T> setParallelism(int parallelism) {
        return parentStream.setParallelism(parallelism);
    }

    @Override
    public List<T> getData() {
        return groupedData.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toList());
    }

    @Override
    public DataStream<T> withWindow(long windowSizeMillis) {
        return parentStream.withWindow(windowSizeMillis);
    }
}