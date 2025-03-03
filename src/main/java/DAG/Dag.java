package DAG;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Dag {
    private List<DagNode> nodes = new ArrayList<>();  // 所有节点
    private Map<String, DagNode> nodeMap = new HashMap<>();  // 通过 ID 快速查找节点


    public void addNode(DagNode node) {
        nodes.add(node);
        nodeMap.put(node.getId(), node);
    }

    public List<DagNode> getNodes() {
        return nodes;
    }

    public DagNode getNodeById(String id) {
        return nodeMap.get(id);
}
}
