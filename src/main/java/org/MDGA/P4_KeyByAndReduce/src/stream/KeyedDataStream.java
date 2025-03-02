package stream;

import java.util.function.BinaryOperator;

/**
 * KeyedDataStream接口定义了按key分组后的数据流操作
 * @param <T> 数据类型
 * @param <K> key的类型
 */
public interface KeyedDataStream<T, K> extends DataStream<T> {
    /**
     * 对分组后的数据进行归约操作
     *
     * @param reducer 归约函数
     * @return 归约后的数据流
     */
    DataStream<T> reduce(BinaryOperator<T> reducer);

    /**
     * 获取分组的key
     *
     * @return key的提取函数
     */
    K getKey();
}