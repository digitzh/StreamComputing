package DAG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 有向无环图(DAG)类
 * 用于表示数据流处理的整体流程，由多个节点和它们之间的连接关系组成
 */
public class Dag {
    private List<DagNode> nodes = new ArrayList<>();       // 存储DAG中的所有节点
    private Map<String, DagNode> nodeMap = new HashMap<>(); // 通过ID快速查找节点的映射

    /**
     * 添加节点到DAG
     * @param node 要添加的节点
     * @throws IllegalArgumentException 当节点为空、ID为空或ID已存在时抛出
     */
    public void addNode(DagNode node) {
        if (node == null) {
            throw new IllegalArgumentException("节点不能为空");
        }
        
        if (node.getId() == null || node.getId().trim().isEmpty()) {
            throw new IllegalArgumentException("节点ID不能为空");
        }
        
        if (nodeMap.containsKey(node.getId())) {
            throw new IllegalArgumentException("节点ID已存在: " + node.getId());
        }
        
        nodes.add(node);
        nodeMap.put(node.getId(), node);
    }

    /**
     * 获取DAG中的所有节点
     * @return 节点列表
     */
    public List<DagNode> getNodes() {
        return nodes;
    }

    /**
     * 通过ID获取节点
     * @param id 节点ID
     * @return 对应的节点
     * @throws IllegalArgumentException 当ID为空或节点不存在时抛出
     */
    public DagNode getNodeById(String id) {
        if (id == null || id.trim().isEmpty()) {
            throw new IllegalArgumentException("节点ID不能为空");
        }
        DagNode node = nodeMap.get(id);
        if (node == null) {
            throw new IllegalArgumentException("找不到ID为" + id + "的节点");
        }
        return node;
    }
    
    /**
     * 验证DAG是否有效
     * 有效的DAG必须：
     * 1. 不为空
     * 2. 所有节点的下游节点必须存在
     * 3. 不存在环路
     * 4. 至少有一个源节点
     * @return 如果DAG有效则返回true，否则返回false
     */
    public boolean validate() {
        if (nodes.isEmpty()) {
            return false; // DAG为空
        }
        
        // 检查每个节点的后继是否都存在
        for (DagNode node : nodes) {
            for (String nextId : node.getNextNodes()) {
                if (!nodeMap.containsKey(nextId)) {
                    return false; // 有后继节点不存在
                }
            }
        }
        
        // 检查是否存在环
        if (hasCycle()) {
            return false;
        }
        
        // 检查是否有源节点
        boolean hasSource = false;
        for (DagNode node : nodes) {
            if (node.getType() == NodeType.SOURCE) {
                hasSource = true;
                break;
            }
        }
        
        return hasSource;
    }
    
    /**
     * 检查DAG中是否存在环
     * @return 如果存在环返回true，否则返回false
     */
    private boolean hasCycle() {
        Set<String> visited = new HashSet<>();  // 记录已访问的节点
        Set<String> inStack = new HashSet<>();  // 记录当前递归栈中的节点
        
        for (DagNode node : nodes) {
            if (!visited.contains(node.getId())) {
                if (hasCycleDFS(node.getId(), visited, inStack)) {
                    return true;
                }
            }
        }
        
        return false;
    }
    
    /**
     * 深度优先搜索检测环
     * @param nodeId 当前访问的节点ID
     * @param visited 已访问的节点集合
     * @param inStack 当前递归栈中的节点集合
     * @return 如果存在环返回true，否则返回false
     */
    private boolean hasCycleDFS(String nodeId, Set<String> visited, Set<String> inStack) {
        visited.add(nodeId);
        inStack.add(nodeId);
        
        DagNode node = nodeMap.get(nodeId);
        for (String nextId : node.getNextNodes()) {
            if (!visited.contains(nextId)) {
                if (hasCycleDFS(nextId, visited, inStack)) {
                    return true;
                }
            } else if (inStack.contains(nextId)) {
                return true; // 发现环
            }
        }
        
        inStack.remove(nodeId); // 回溯时从栈中移除
        return false;
    }
}
