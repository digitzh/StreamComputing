import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 流处理算子的基类
 */
public abstract class StreamOperator implements Runnable {
    protected volatile boolean isRunning = true;
    protected int parallelism;
    protected ExecutorService executorService;

    public StreamOperator(int parallelism) {
        this.parallelism = parallelism;
        this.executorService = Executors.newFixedThreadPool(parallelism);
    }

    /**
     * 设置算子的并行度
     * @param newParallelism 新的并行度
     */
    public void setParallelism(int newParallelism) {
        if (newParallelism <= 0) {
            throw new IllegalArgumentException("并行度必须大于0");
        }
        
        // 如果新的并行度与当前并行度相同，则不需要调整
        if (newParallelism == this.parallelism) {
            return;
        }

        System.out.println(String.format("[%s] 调整并行度: %d -> %d", 
            this.getClass().getSimpleName(), this.parallelism, newParallelism));

        // 关闭当前的ExecutorService
        executorService.shutdownNow();

        // 更新并行度
        this.parallelism = newParallelism;

        // 创建新的ExecutorService
        this.executorService = Executors.newFixedThreadPool(newParallelism);

        // 重新分配工作线程
        rebalanceWorkers();
    }

    /**
     * 重新分配工作线程
     * 子类需要实现这个方法来处理并行度变化后的工作线程重新分配
     */
    protected abstract void rebalanceWorkers();

    /**
     * 停止算子
     */
    public void stop() {
        isRunning = false;
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    /**
     * 获取当前并行度
     */
    public int getParallelism() {
        return parallelism;
    }
}