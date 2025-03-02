// StreamingJob.java

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class StreamingJob {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("工作目录: " + System.getProperty("user.dir"));
        // 初始化数据流
        DataStream<String> sourceStream = new DataStream<>();
        DataStream<String> mappedStream = new DataStream<>();

        // 初始化算子
        // 1. source算子（从Kafka读取）
        Source source = new Source(sourceStream, "input-topic");

        // 2. map算子（示例：将输入转换为大写）
        MapOperator<String, String> map = new MapOperator<>(
                sourceStream,
                mappedStream,
                String::toUpperCase, // 示例处理逻辑：转大写
                1  // 设置并行度
        );

        // 3. keyBy算子（示例：按第一个逗号分隔的字段分组）
        KeyByOperator<String, String> keyBy = new KeyByOperator<>(
                mappedStream,
                s -> s.split(",")[0],  // 假设输入格式为 "key,value"
                1  // 设置并行度
        );
        Thread keyByThread = new Thread(keyBy);
        keyByThread.start();

        // 模拟Kafka生产者
        // DataGenerator
        DataGenerator<String> generator = new DataGenerator<>(
                sourceStream,
                () -> {
                    String[] keys = {"u1", "u2", "u3", "u4", "u5"};
                    return keys[(int)(Math.random() * keys.length)] + "," + System.currentTimeMillis();
                },
                500 // 每500ms生成一条数据
        );
        new Thread(generator).start();

        KeyedDataStream<String, String> keyedStreams = keyBy.getKeyedStreams();

        // 3. reduce算子（示例：将分组后的数据进行聚合）
        DataStream<String> reducedStream = new DataStream<>();
        ReduceOperator<String, String> reduce = new ReduceOperator<>(
                keyedStreams,
                reducedStream,
                (acc, current) -> acc + "|" + current  // 示例reduce逻辑：字符串拼接
        );

        // 4. Sink算子（示例：将结果写入文件）
        Sink sink = new Sink(reducedStream, "output.txt");

        // 启动线程
        // 1. source
        Thread sourceThread = new Thread(source);
        sourceThread.start();
        // 2. map
        //     单线程
//        Thread mapThread = new Thread(map);
//        mapThread.start();
        //     多线程
        List<Thread> mapThreads = new ArrayList<>();
        for (int i = 0; i < map.getParallelism(); i++) {
            Thread t = new Thread(map);
            mapThreads.add(t);
            t.start();
        }
        // 3. reduce
        Thread reduceThread = new Thread(reduce);
        reduceThread.start();
        // 4. sink
        Thread sinkThread = new Thread(sink);
        sinkThread.start();

        // 为每个key创建处理流水线
        ConcurrentHashMap<String, Sink> sinks = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Thread> processorThreads = new ConcurrentHashMap<>();

        // 运行 60 秒后停止
        Thread.sleep(60_000);
        // 1. source
        source.stop();
        // 2. map
        map.stop();
        // 3. reduce
        reduce.stop();
        // 4. sink
        sinks.values().forEach(Sink::stop);
        processorThreads.values().forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        sink.stop();

        // 等待线程结束
        sourceThread.join();
//        mapThread.join(); // 单线程
        for (Thread t : mapThreads) {
            t.join();
        }
        reduceThread.join();
        sinkThread.join();
    }
}