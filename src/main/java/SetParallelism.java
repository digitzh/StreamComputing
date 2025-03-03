public class SetParallelism {
    // 并发度控制核心基类
    public abstract class StreamOperator<T> {
        private int parallelism = 1;
        private StreamOperator<?> upstream; // 上游引用
        protected List<Worker<T>> workers = new ArrayList<>();
        private final KeyedDataRouter router = new KeyedDataRouter();

        private List<Thread> workerThreads = new ArrayList<>();//线程管理

        public void setUpstream(StreamOperator<?> upstream) {
            this.upstream = upstream;
        }

        public StreamOperator<?> getUpstream() {
            return this.upstream;
        }
        // 设置并发度入口
        public void setParallelism(int parallelism) {
            if (this instanceof ReduceOperator && getUpstream() instanceof KeyByOperator) {
                if (parallelism != getUpstream().getParallelism()) {
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

    public class JobExample {
        public static void main(String[] args) {
            // 创建算子
            StreamOperator<String> source = new KafkaSourceOperator<>("input-topic", new StringDeserialization());
            StreamOperator<String> map = new MapOperator<>(String::toUpperCase);
            StreamOperator<String> keyBy = new KeyByOperator<>(s -> s.split(":")[0]);
            StreamOperator<String> reduce = new ReduceOperator<>((a, b) -> a + "," + b);
            StreamOperator<Void> sink = new FileSinkOperator<>("/output", new TextFileWriterFactory());

            // 构建流水线并设置并发度
            DataStream<String> stream = new DataStream<>(source)
                    .withParallelism(1)  // Source并发度
                    .applyOperator(map)
                    .withParallelism(1)  // Map并发度
                    .applyOperator(keyBy)
                    .withParallelism(1)  // KeyBy并发度
                    .applyOperator(reduce)
                    .withParallelism(1)  // Reduce必须与KeyBy相同
                    .applyOperator(sink)
                    .withParallelism(1); // Sink并发度
        }
    }
}
