package DAG;

public class TestDagParser {
    public static void main(String[] args) throws Exception {
        Dag dag = DagParser.parse("src/main/resources/dag-config.yaml");
        for (DagNode node : dag.getNodes()) {
            System.out.println("Node: " + node.getId());
            System.out.println("Type: " + node.getType());
            System.out.println("Parallelism: " + node.getParallelism());
            System.out.println("Config: " + node.getConfig());
            System.out.println("Next Nodes: " + node.getNextNodes());
            System.out.println("-----------------------");
        }
    }
}
