import java.util.function.Function;

/**
 * 流式Map算子（支持泛型）
 */
public class MapOperator<T, R> implements Runnable {
    private final DataStream<T> inputStream;
    private final DataStream<R> outputStream;
    private final Function<T, R> mapper;
    private volatile boolean isRunning = true;

    public MapOperator(DataStream<T> inputStream,
                       DataStream<R> outputStream,
                       Function<T, R> mapper) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.mapper = mapper;
    }

    @Override
    public void run() {
        try {
            while (isRunning) {
                T input = inputStream.poll();
                R transformed = mapper.apply(input);
                outputStream.emit(transformed);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        isRunning = false;
    }
}
