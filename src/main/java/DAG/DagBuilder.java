package DAG;

import DAG.config.SourceConfig;
import DAG.config.OperatorConfig;
import DAG.config.SinkConfig;
import DAG.config.JobConfig;
import DAG.config.DagConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * DAG构建器
 * 负责从配置对象构建DAG实例
 */
public class DagBuilder {
    
    /**
     * 从配置对象构建DAG
     * @param dagConfig DAG配置对象
     * @return 构建的DAG实例
     * @throws IllegalArgumentException 当配置无效或DAG验证失败时抛出
     */
    public static Dag buildFromConfig(DagConfig dagConfig) {
        if (dagConfig == null || dagConfig.getJob() == null) {
            throw new IllegalArgumentException("配置不能为空");
        }
        
        JobConfig jobConfig = dagConfig.getJob();
        Dag dag = new Dag();
        
        // 添加Sources
        if (jobConfig.getSources() != null) {
            for (SourceConfig source : jobConfig.getSources()) {
                DagNode node = convertSourceConfig(source);
                dag.addNode(node);
            }
        }
        
        // 添加Operators
        if (jobConfig.getOperators() != null) {
            for (OperatorConfig operator : jobConfig.getOperators()) {
                DagNode node = convertOperatorConfig(operator);
                dag.addNode(node);
            }
        }
        
        // 添加Sinks
        if (jobConfig.getSinks() != null) {
            for (SinkConfig sink : jobConfig.getSinks()) {
                DagNode node = convertSinkConfig(sink);
                dag.addNode(node);
            }
        }
        
        // 设置节点间连接关系
        setNodeConnections(dag, jobConfig);
        
        // 验证DAG
        if (!dag.validate()) {
            throw new IllegalArgumentException("DAG配置无效");
        }
        
        return dag;
    }
    
    /**
     * 将源配置转换为DAG节点
     * @param source 源配置
     * @return 对应的DAG节点
     */
    private static DagNode convertSourceConfig(SourceConfig source) {
        DagNode node = new DagNode();
        node.setId(source.getId());
        node.setType(NodeType.SOURCE);
        node.setParallelism(source.getParallelism());
        
        // 设置特定配置
        node.addConfig("type", source.getType());
        if (source.getTopic() != null) {
            node.addConfig("topic", source.getTopic());
        }
        if (source.getBootstrapServers() != null) {
            node.addConfig("bootstrap-servers", source.getBootstrapServers());
        }
        
        return node;
    }
    
    /**
     * 将算子配置转换为DAG节点
     * @param operator 算子配置
     * @return 对应的DAG节点
     */
    private static DagNode convertOperatorConfig(OperatorConfig operator) {
        DagNode node = new DagNode();
        node.setId(operator.getId());
        node.setParallelism(operator.getParallelism());
        
        // 根据算子类型设置节点类型
        String operatorType = operator.getType().toLowerCase();
        switch (operatorType) {
            case "map":
                node.setType(NodeType.MAP);
                break;
            case "keyby":
                node.setType(NodeType.KEYBY);
                break;
            case "reduce":
                node.setType(NodeType.REDUCE);
                break;
            default:
                throw new IllegalArgumentException("不支持的算子类型: " + operatorType);
        }
        
        // 设置特定配置
        node.addConfig("type", operator.getType());
        if (operator.getFunction() != null) {
            node.addConfig("function", operator.getFunction());
        }
        
        // 为KeyBy算子设置特定配置
        if (node.getType() == NodeType.KEYBY && operator.getKeySelector() != null) {
            node.addConfig("key-selector", operator.getKeySelector());
        }
        
        return node;
    }
    
    /**
     * 将接收器配置转换为DAG节点
     * @param sink 接收器配置
     * @return 对应的DAG节点
     */
    private static DagNode convertSinkConfig(SinkConfig sink) {
        DagNode node = new DagNode();
        node.setId(sink.getId());
        node.setType(NodeType.SINK);
        node.setParallelism(sink.getParallelism());
        
        // 设置特定配置
        node.addConfig("type", sink.getType());
        if (sink.getPath() != null) {
            node.addConfig("path", sink.getPath());
        }
        
        return node;
    }
    
    /**
     * 设置节点间的连接关系
     * @param dag DAG实例
     * @param jobConfig 作业配置
     */
    private static void setNodeConnections(Dag dag, JobConfig jobConfig) {
        // 设置Source的连接
        if (jobConfig.getSources() != null) {
            for (SourceConfig source : jobConfig.getSources()) {
                if (source.getNext() != null && !source.getNext().isEmpty()) {
                    DagNode sourceNode = dag.getNodeById(source.getId());
                    String[] nextIds = source.getNext().split(",");
                    for (String nextId : nextIds) {
                        sourceNode.addNextNode(nextId.trim());
                    }
                }
            }
        }
        
        // 设置Operator的连接
        if (jobConfig.getOperators() != null) {
            for (OperatorConfig operator : jobConfig.getOperators()) {
                if (operator.getNext() != null && !operator.getNext().isEmpty()) {
                    DagNode operatorNode = dag.getNodeById(operator.getId());
                    String[] nextIds = operator.getNext().split(",");
                    for (String nextId : nextIds) {
                        operatorNode.addNextNode(nextId.trim());
                    }
                }
            }
        }
    }
} 