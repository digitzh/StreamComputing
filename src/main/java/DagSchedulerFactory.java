public class DagSchedulerFactory {
    public static DagScheduler createScheduler(String type, int threadPoolSize) {
        switch (type.toLowerCase()) {
            case "topological":
                return new TopologicalDagScheduler();
            // 可以添加其他类型的调度器
            default:
                throw new IllegalArgumentException("未知的调度器类型: " + type);
        }
    }
}