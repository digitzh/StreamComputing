import java.util.HashMap;
import java.util.Map;


public class StreamContext {
    private final Map<String, DataStream<?>> streams = new HashMap<>();
    private final Map<String, Runnable> operators = new HashMap<>();
    private final Map<String, KeyedDataStream<?, ?>> keyedStreams = new HashMap<>();
    private final Map<String, Thread> threads = new HashMap<>();

     public void createStream(String nodeId) {
        streams.put(nodeId, new DataStream<>());
    }

    public DataStream<?> getInputStream(String nodeId) {
        return streams.get(nodeId);
    }

    public DataStream<?> getOutputStream(String nodeId) {
        return streams.get(nodeId);
    }

    public void setKeyedStream(String nodeId, KeyedDataStream<?, ?> keyedStream) {
        keyedStreams.put(nodeId, keyedStream);
    }

    public KeyedDataStream<?, ?> getKeyedStream(String nodeId) {
        return keyedStreams.get(nodeId);
    }

    public void registerOperator(String nodeId, Runnable operator) {
        operators.put(nodeId, operator);

        // 如果是 KeyByOperator，保存其 KeyedDataStream
        if (operator instanceof KeyByOperator) {
            @SuppressWarnings("unchecked")
            KeyByOperator<String, String> keyByOp = (KeyByOperator<String, String>) operator;
            setKeyedStream(nodeId, keyByOp.getKeyedStreams());
        }
    }


    public void startOperator(String nodeId) {
        Runnable operator = operators.get(nodeId);
        if (operator != null) {
            Thread thread = new Thread(operator);
            threads.put(nodeId, thread);
            thread.start();
        }
    }

    public void stopAll() {
        operators.forEach((nodeId, operator) -> {
            if (operator instanceof DataGenerator) {
                ((DataGenerator<?>) operator).stop();
            } else if (operator instanceof MapOperator) {
                ((MapOperator<?, ?>) operator).stop();
            } else if (operator instanceof KeyByOperator) {
                ((KeyByOperator<?, ?>) operator).stop();
            } else if (operator instanceof ReduceOperator) {
                ((ReduceOperator<?, ?>) operator).stop();
            } else if (operator instanceof FileSink) {
                ((FileSink) operator).stop();
            }
        });

        threads.forEach((nodeId, thread) -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

}