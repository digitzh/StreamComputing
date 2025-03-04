package DAG;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * DAG图中的节点类
 * 表示数据处理流程中的一个处理单元，可以是数据源、操作算子或数据接收器
 */
public class DagNode {
    private String id;                                    // 节点唯一标识
    private NodeType type;                                // 节点类型
    private List<String> nextNodes = new ArrayList<>();   // 下游节点ID列表
    private int parallelism = 1;                          // 节点的并行度，默认为1
    private Map<String, String> config = new HashMap<>(); // 节点的配置参数

    /**
     * 默认构造函数
     */
    public DagNode() {
    }
    
    /**
     * 带参数的构造函数
     * @param id 节点ID
     * @param type 节点类型
     */
    public DagNode(String id, NodeType type) {
        this.id = id;
        this.type = type;
    }

    /**
     * 获取节点ID
     * @return 节点ID
     */
    public String getId() {
        return id;
    }
    
    /**
     * 设置节点ID
     * @param id 节点ID
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * 获取节点类型
     * @return 节点类型
     */
    public NodeType getType() {
        return type;
    }

    /**
     * 设置节点类型
     * @param type 节点类型枚举值
     */
    public void setType(NodeType type) {
        this.type = type;
    }
    
    /**
     * 通过字符串设置节点类型
     * @param typeStr 节点类型字符串
     */
    public void setType(String typeStr) {
        this.type = NodeType.fromString(typeStr);
    }

    /**
     * 获取下游节点ID列表
     * @return 下游节点ID列表
     */
    public List<String> getNextNodes() {
        return nextNodes;
    }

    /**
     * 设置下游节点ID列表
     * @param nextNodes 下游节点ID列表
     */
    public void setNextNodes(List<String> nextNodes) {
        this.nextNodes = nextNodes;
    }
    
    /**
     * 添加下游节点ID
     * 如果已存在则不重复添加
     * @param nodeId 要添加的下游节点ID
     */
    public void addNextNode(String nodeId) {
        if (!nextNodes.contains(nodeId)) {
            nextNodes.add(nodeId);
        }
    }

    /**
     * 获取节点并行度
     * @return 节点并行度
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * 设置节点并行度
     * @param parallelism 并行度，必须大于0
     * @throws IllegalArgumentException 当并行度小于等于0时抛出
     */
    public void setParallelism(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("并行度必须大于0");
        }
        this.parallelism = parallelism;
    }

    /**
     * 获取节点配置
     * @return 节点配置参数映射
     */
    public Map<String, String> getConfig() {
        return config;
    }

    /**
     * 设置节点配置
     * @param config 节点配置参数映射
     */
    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
    
    /**
     * 添加配置参数
     * @param key 配置参数键
     * @param value 配置参数值
     */
    public void addConfig(String key, String value) {
        this.config.put(key, value);
    }
}
