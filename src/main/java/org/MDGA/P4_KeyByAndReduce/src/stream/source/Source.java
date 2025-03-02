package stream.source;

import stream.DataStream;

/**
 * 数据源接口，定义了数据流的输入源操作
 * @param <T> 数据类型
 */
public interface Source<T> {
    /**
     * 从数据源读取数据并创建数据流
     *
     * @return 包含源数据的数据流
     */
    DataStream<T> getDataStream();

    /**
     * 关闭数据源连接
     */
    void close();
}