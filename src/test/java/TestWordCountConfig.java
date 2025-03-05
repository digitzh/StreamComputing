import DAG.Dag;
import DAG.DagParser;

public class TestWordCountConfig {
    public static void main(String[] args) throws Exception {
        System.out.println("工作目录: " + System.getProperty("user.dir"));

        // 解析DAG配置
        Dag dag = DagParser.parse("src/main/resources/dag-config.yaml");

        // 创建调度器
        DagScheduler scheduler = new TopologicalDagScheduler();

        // 初始化调度器
        scheduler.init(dag);

        try {
            // 开始执行
            scheduler.start();

            // 运行一段时间后停止
            Thread.sleep(30_000);
        } finally {
            // 停止执行
            scheduler.stop();
        }
    }
}
