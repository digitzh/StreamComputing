import DAG.Dag;

public interface DagScheduler {
    /**
     * 初始化调度器
     * @param dag 要调度的DAG
     */
    void init(Dag dag);

    /**
     * 开始调度执行
     */
    void start();

    /**
     * 停止调度执行
     */
    void stop();
}



