package DAG;

import DAG.config.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class DagParser {
    public static Dag parse(String configPath) throws IOException {
        // 1. 读取 YAML 文件并反序列化为 DagConfig 对象
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        DagConfig dagConfig = mapper.readValue(new File(configPath), DagConfig.class);

        // 2. 构建空的 DAG
        Dag dag = new Dag();
        JobConfig job = dagConfig.getJob();

        // 3. 解析所有 Source 节点
        for (SourceConfig source : job.getSources()) {
            DagNode node = new DagNode();
            node.setId(source.getId());
            node.setType("source");
            node.setParallelism(source.getParallelism());
            node.setNextNodes(Collections.singletonList(source.getNext()));
            node.setConfig(Map.of(
                    "topic", source.getTopic(),
                    "bootstrap-servers", source.getBootstrapServers()
            ));
            dag.addNode(node);
        }

        // 4. 解析所有 Operator 节点（Map、KeyBy、Reduce）
        for (OperatorConfig operator : job.getOperators()) {
            DagNode node = new DagNode();
            node.setId(operator.getId());
            node.setType(operator.getType());
            node.setParallelism(operator.getParallelism());
            node.setNextNodes(Collections.singletonList(operator.getNext()));
            node.setConfig(Map.of(
                    "function", operator.getFunction(),
                    "key-selector", operator.getKeySelector()
            ));
            dag.addNode(node);
        }

         // 5. 解析所有 Sink 节点
        for (SinkConfig sink : job.getSinks()) {
            DagNode node = new DagNode();
            node.setId(sink.getId());
            node.setType("sink");
            node.setParallelism(sink.getParallelism());
            node.setConfig(Map.of("path", sink.getPath()));
            dag.addNode(node);
        }

        return dag;
    }
}
