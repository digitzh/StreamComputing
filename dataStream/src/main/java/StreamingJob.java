// StreamingJob.java

import java.io.File;
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
        List<Thread> mapThreads = new ArrayList<>();
        for (int i = 0; i < map.getParallelism(); i++) {
            Thread t = new Thread(map);
            mapThreads.add(t);
            t.start();
        }
        // 3. keyBy算子（示例：按第一个逗号分隔的字段分组）
        KeyByOperator<String, String> keyBy = new KeyByOperator<>(
                mappedStream,
                s -> s.split(",")[0],  // 假设输入格式为 "key,value"
                1  // 设置并行度
        );
        Thread keyByThread = new Thread(keyBy);
        keyByThread.start();

        // 模拟Kafka生产者
        // 1. 简单的测试数据
        sourceStream.emit("user,123");
        sourceStream.emit("order,abc");
        // 2. String数组类型数据
        new Thread(() -> {
            try {
                // 等待流水线启动
                Thread.sleep(2000);

                // 发送测试数据
                String[] testData = {
                        "user,456",
                        "order,def",
                        "user,789",
                        "order,ghi"
                };

                // 实际应通过Kafka生产者发送
                for (String data : testData) {
                    sourceStream.emit(data);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

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
        Thread sourceThread = new Thread(source);
        sourceThread.start();
        Thread mapThread = new Thread(map);
        mapThread.start();
        Thread reduceThread = new Thread(reduce);
        reduceThread.start();
        Thread sinkThread = new Thread(sink);
        sinkThread.start();

        // 为每个key创建处理流水线
        ConcurrentHashMap<String, Sink> sinks = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Thread> processorThreads = new ConcurrentHashMap<>();

        // 启动sink线程
        // 这部分的作用是单独测试keyBy（不启用reduce）
        // 效果是将keyBy的输出写入文件，每个key对应一个文件
        // 文件名格式为：output_<key>.txt
        // 例如：output_user.txt, output_order.txt
//        keyedStreams.forEach((key, stream) -> {
//            System.out.println("正在为key: " + key + "创建sink");
//            String path = "output_" + key + ".txt";
//            System.out.println("输出路径: " + new File(path).getAbsolutePath());
//            System.out.println(key + "的sink线程启动");
//
//            Thread t = new Thread(()-> {
//                Sink keyed_sink = new Sink(stream, path);
//                keyed_sink.run();
//                sinks.put(key, keyed_sink);
//            });
//            processorThreads.put(key, t);
//            t.start();
//        });

        // 运行 60 秒后停止
        Thread.sleep(60_000);
        source.stop();
        map.stop();

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
        mapThread.join();
        for (Thread t : mapThreads) {
            t.join();
        }
        sinkThread.join();
    }
}