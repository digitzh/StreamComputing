import java.util.function.Function;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 流式Map算子（支持泛型）
 * 该算子用于对输入流中的每个元素应用转换函数，将其映射为新的输出元素
 * 支持并行处理，可以通过设置parallelism参数来控制并行度
 */
public class MapOperator<T, R> extends StreamOperator {
    // 输入数据流
    private final DataStream<T> inputStream;
    // 输出数据流
    private final DataStream<R> outputStream;
    // 转换函数，将类型T转换为类型R
    private final Function<T, R> mapper;

    /**
     * 构造Map算子
     * @param inputStream 输入数据流
     * @param outputStream 输出数据流
     * @param mapper 转换函数
     * @param parallelism 并行度
     */
    public MapOperator(DataStream<T> inputStream,
                       DataStream<R> outputStream,
                       Function<T, R> mapper,
                       int parallelism) {
        super(parallelism);
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.mapper = mapper;
    }

    /**
     * 重新平衡工作线程
     * 当并行度发生变化时，重新分配工作线程处理数据
     */
    @Override
    protected void rebalanceWorkers() {
        for (int i = 0; i < parallelism; i++) {
            final int workerId = i;
            executorService.submit(() -> {
                try {
                    while (isRunning) {
                        T input = inputStream.poll();
                        if (input != null) {
                            R transformed = mapper.apply(input);
                            System.out.println(String.format("[Map-Worker-%d] Mapped %s to %s", workerId, input, transformed));
                            outputStream.emit(transformed);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    /**
     * 运行Map算子
     * 启动工作线程并持续运行，直到收到停止信号
     */
    @Override
    public void run() {
        rebalanceWorkers();
        while (isRunning) {
            try {
                Thread.sleep(100); // 避免空转
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
