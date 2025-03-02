package stream;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * 基础数据流实现类
 * @param <T> 数据类型
 */
public class BaseDataStream<T> implements DataStream<T> {
    private final List<T> data;
    private int parallelism = 1;
    private long windowSize = -1;

    public BaseDataStream(List<T> data) {
        this.data = new ArrayList<>(data);
    }

    @Override
    public <K> KeyedDataStream<T, K> keyBy(Function<T, K> keyExtractor) {
        return new KeyedDataStreamImpl<>(this, keyExtractor);
    }

    @Override
    public DataStream<T> setParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("并行度必须大于0");
        }
        this.parallelism = parallelism;
        return this;
    }

    @Override
    public DataStream<T> withWindow(long windowSizeMillis) {
        if (windowSizeMillis <= 0) {
            throw new IllegalArgumentException("窗口大小必须大于0");
        }
        this.windowSize = windowSizeMillis;
        return this;
    }

    /**
     * 获取数据列表
     */
    public List<T> getData() {
        return data;
    }

    /**
     * 获取并行度
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * 获取窗口大小
     */
    public long getWindowSize() {
        return windowSize;
    }
}