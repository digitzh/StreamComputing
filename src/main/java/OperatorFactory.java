
import DAG.DagNode;

public class OperatorFactory {
    public static Runnable createOperator(DagNode node, StreamContext context, String prevNodeId) {
        switch (node.getType()) {
            case SOURCE:
                DataStream<String> sourceOutputStream = (DataStream<String>) context.getOutputStream(node.getId());
                // 使用DataGenerator替代Source
                return new DataGenerator<>(
                        sourceOutputStream,
                        () -> {
                            String[] keys = {"U1", "U2", "U3", "U4", "U5"};
                            return keys[(int)(Math.random() * keys.length)] + "," + System.currentTimeMillis();
                        },
                        500 // 每500ms生成一条数据
                );

            case MAP:
                return new MapOperator<>(
                        (DataStream<String>) context.getInputStream(prevNodeId),
                        (DataStream<String>) context.getOutputStream(node.getId()),
                        input -> {
                            String function = node.getConfig().get("function");
                            if ("toLowerCase".equals(function)) {
                                return input.toLowerCase();
                            }
                            return input;
                        },
                        node.getParallelism()
                );


            case KEYBY:
                return new KeyByOperator<>(
                        (DataStream<String>) context.getInputStream(prevNodeId),
                        input -> {
                            String keySelector = node.getConfig().get("key-selector");
                            if ("word".equals(keySelector)) {
                                return input.split(",")[0];
                            }
                            return input;
                        },
                        node.getParallelism()
                );

            case REDUCE:
                @SuppressWarnings("unchecked")
                KeyedDataStream<String, String> keyedStream =
                        (KeyedDataStream<String, String>) context.getKeyedStream(prevNodeId);
                if (keyedStream == null) {
                    throw new IllegalStateException("Reduce operator requires KeyedDataStream input");
                }
                return new ReduceOperator<>(
                        keyedStream,
                        (DataStream<String>) context.getOutputStream(node.getId()),
                        (acc, curr) -> {
                            String function = node.getConfig().get("function");
                            if ("sum".equals(function)) {
                                return acc + "|" + curr;
                            }
                            return curr;
                        }
                );

            case SINK:
                return new FileSink(
                        (DataStream<String>) context.getInputStream(prevNodeId),
                        node.getConfig().get("path")
                );

            default:
                throw new IllegalArgumentException("未知的算子类型: " + node.getType());
        }
    }
}

