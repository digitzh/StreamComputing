package stream;

import java.util.List;
import java.util.function.Function;

/**
 * DataStream接口定义了流处理系统中的基本数据流操作
 */
public interface DataStream<T> {
    /**
     * 根据key函数对数据流进行分组
     *
     * @param keyExtractor 从数据中提取key的函数
     * @param <K> key的类型
     * @return 按key分组后的数据流
     */
    <K> KeyedDataStream<T, K> keyBy(Function<T, K> keyExtractor);

    /**
     * 设置算子的并行度
     *
     * @param parallelism 并行度
     * @return 设置并行度后的数据流
     */
    DataStream<T> setParallelism(int parallelism);

    /**
     * 设置处理时间窗口大小
     *
     * @param windowSizeMillis 窗口大小（毫秒）
     * @return 设置窗口后的数据流
     */
    DataStream<T> withWindow(long windowSizeMillis);

    /**
     * 获取数据流中的数据
     *
     * @return 数据列表
     */
    List<T> getData();
}