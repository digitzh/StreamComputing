import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 无界数据流抽象，通过阻塞队列实现数据缓冲
 */
public class DataStream<T> {
    private final BlockingQueue<T> buffer = new LinkedBlockingQueue<>();

    // 向流中发送数据
    public void emit(T element) {
        buffer.add(element);
    }

    // 从流中消费数据（阻塞式）
    public T poll() throws InterruptedException {
        return buffer.take();
    }
}