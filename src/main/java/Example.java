import DAG.Dag;
import DAG.DagParser;

import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        // 解析DAG配置
        Dag dag = DagParser.parse("dataStream/src/main/resources/dag-config.yaml");

        // 创建调度器
        DagScheduler scheduler = DagSchedulerFactory.createScheduler("topological", 4);

        // 初始化调度器
        scheduler.init(dag);

        try {
            // 开始执行
            scheduler.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 停止执行
            scheduler.stop();
        }
    }
}