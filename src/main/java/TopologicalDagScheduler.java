import DAG.Dag;
import DAG.DagNode;
import DAG.NodeType;

import java.util.*;

public class TopologicalDagScheduler implements DagScheduler {
    private final StreamContext context;
    private Dag dag;

    public TopologicalDagScheduler() {
        this.context = new StreamContext();
    }


    @Override
    public void init(Dag dag) {
        this.dag = dag;
        // 为每个节点创建数据流
        for (DagNode node : dag.getNodes()) {
            context.createStream(node.getId());
        }

        // 创建并注册所有算子
        for (DagNode node : dag.getNodes()) {
            // 找到前一个节点
            String prevNodeId = findPreviousNodeId(node);

            // 创建并注册算子
            Runnable operator = OperatorFactory.createOperator(node, context, prevNodeId);
            context.registerOperator(node.getId(), operator);
        }
    }

    private String findPreviousNodeId(DagNode currentNode) {
        if (currentNode.getType() == NodeType.SOURCE) {
            return null;
        }

        // 遍历所有节点，找到指向当前节点的节点
        for (DagNode node : dag.getNodes()) {
            if (node.getNextNodes().contains(currentNode.getId())) {
                return node.getId();
            }
        }
        throw new IllegalStateException("找不到节点 " + currentNode.getId() + " 的前置节点");
    }


    @Override
    public void start() {
        // 按照拓扑排序顺序启动算子
        List<String> sortedNodes = topologicalSort();
        for (String nodeId : sortedNodes) {
            context.startOperator(nodeId);
        }
    }

    @Override
    public void stop() {
        context.stopAll();
    }

    private List<String> topologicalSort() {
        Map<String, Integer> inDegree = new HashMap<>();
        Queue<String> queue = new LinkedList<>();
        List<String> result = new ArrayList<>();

        // 初始化入度
        for (DagNode node : dag.getNodes()) {
            inDegree.put(node.getId(), 0);
        }

        // 计算入度
        for (DagNode node : dag.getNodes()) {
            for (String nextId : node.getNextNodes()) {
                inDegree.merge(nextId, 1, Integer::sum);
            }
        }

        // 将入度为0的节点加入队列
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                queue.offer(entry.getKey());
            }
        }

        // 拓扑排序
        while (!queue.isEmpty()) {
            String nodeId = queue.poll();
            result.add(nodeId);

            DagNode node = dag.getNodeById(nodeId);
            for (String nextId : node.getNextNodes()) {
                inDegree.merge(nextId, -1, Integer::sum);
                if (inDegree.get(nextId) == 0) {
                    queue.offer(nextId);
                }
            }
        }

        return result;
    }

} 