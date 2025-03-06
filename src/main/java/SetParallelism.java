import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SetParallelism {
    // 并发度控制核心基类
    public abstract class StreamOperator<T> {
        private int parallelism = 1;
        private StreamOperator<?> upstream; // 上游引用
        protected List<Worker<T>> workers = new ArrayList<>();
        private final KeyedDataRouter router = new KeyedDataRouter();
        private List<Thread> workerThreads = new ArrayList<>();

        public int getParallelism() {
            return parallelism;
        }

        protected abstract Worker<T> createWorker(int workerId);

        public void setUpstream(StreamOperator<?> upstream) {
            this.upstream = upstream;
        }

        public StreamOperator<?> getUpstream() {
            return this.upstream;
        }
        // 设置并发度入口
        public void setParallelism(int parallelism) {
            if (this.getClass().equals(ReduceOperator.class) && getUpstream() != null && getUpstream().getClass().equals(KeyByOperator.class)) {
                StreamOperator<?> upstream = getUpstream();
                if (parallelism != upstream.getParallelism()) {
                    throw new IllegalArgumentException("Reduce算子并发度必须与KeyBy算子保持一致");
                }
            }

            if (parallelism < 1) {
                throw new IllegalArgumentException("并发度必须≥1");
            }
            this.parallelism = parallelism;
            rebalanceWorkers();
        }

        // 动态调整Worker
        private void rebalanceWorkers() {
            workerThreads.forEach(Thread::interrupt);
            workerThreads.clear();
            workers.clear();
            for (int i = 0; i < parallelism; i++) {
                Worker<T> worker = createWorker(i);
                workers.add(worker);
                Thread workerThread = new Thread(worker);
                workerThreads.add(workerThread);
                workerThread.start();
            }
            router.updateWorkers(parallelism); // 更新路由表
        }

        // 数据分发逻辑
        protected void dispatchRecord(StreamRecord<T> record) {
            int workerIndex = router.route(record.getKey(), parallelism);
            workers.get(workerIndex).addRecord(record);
        }
        // 修改Worker接口实现队列处理
        public abstract class Worker<T> implements Runnable {
            protected final BlockingQueue<StreamRecord<T>> queue = new LinkedBlockingQueue<>();
            protected final int workerId;

            public Worker(int workerId) {
                this.workerId = workerId;
            }

            public void addRecord(StreamRecord<T> record) {
                queue.offer(record);
            }

            @Override
            public void run() {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        StreamRecord<T> record = queue.poll(100, TimeUnit.MILLISECONDS);
                        if (record != null) {
                            processRecord(record);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            protected abstract void processRecord(StreamRecord<T> record);
        }
    }

    // 并发度路由核心
    public class KeyedDataRouter {
        private final TreeMap<Integer, Integer> hashRing = new TreeMap<>();
        private static final int VIRTUAL_NODES = 1000;

        public void updateWorkers(int workerCount) {
            hashRing.clear();
            for (int i = 0; i < workerCount; i++) {
                for (int j = 0; j < VIRTUAL_NODES; j++) {
                    String nodeKey = "worker-" + i + "-vn-" + j;
                    int hash = nodeKey.hashCode();
                    hashRing.put(hash, i);
                }
            }
        }

        public int route(Object key, int workerCount) {
            int keyHash = key.hashCode();
            Map.Entry<Integer, Integer> entry = hashRing.ceilingEntry(keyHash);
            entry = entry != null ? entry : hashRing.firstEntry();
            return entry.getValue() % workerCount;
        }
    }

    // 数据流构建类
    public class DataStream<T> {
        private StreamOperator<T> currentOperator;

        public DataStream(StreamOperator<T> operator) {
            this.currentOperator = operator;
        }
        // 设置当前算子并发度
        public DataStream<T> withParallelism(int parallelism) {
            currentOperator.setParallelism(parallelism);
            return this;
        }

        // 连接新算子
        public <R> DataStream<R> applyOperator(StreamOperator<R> nextOperator) {
            // 建立上下游关系
            nextOperator.setUpstream(this.currentOperator);
            return new DataStream<>(nextOperator);
        }
    }
}
