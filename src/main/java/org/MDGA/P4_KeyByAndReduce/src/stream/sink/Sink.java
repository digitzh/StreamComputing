package stream.sink;

/**
 * 数据输出接口，定义了数据流的输出操作
 * @param <T> 数据类型
 */
public interface Sink<T> {
    /**
     * 写入数据到目标存储
     *
     * @param data 要写入的数据
     */
    void write(T data);

    /**
     * 关闭输出连接
     */
    void close();
}