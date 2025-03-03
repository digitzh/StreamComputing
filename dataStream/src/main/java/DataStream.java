
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 无界数据流抽象，通过阻塞队列实现数据缓冲
 */
public class    DataStream<T> {
    private final BlockingQueue<T> buffer = new LinkedBlockingQueue<>();

    // 向流中发送数据
    public void emit(T element) {
        buffer.add(element);
    }

    // 从流中消费数据（阻塞式）
    public T poll() throws InterruptedException {
        return buffer.take();
    }
    // 新增带超时的 poll 方法
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
        return buffer.poll(timeout, unit);
    }
}

