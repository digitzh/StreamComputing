// StreamingJob.java

public class StreamingJob {
    public static void main(String[] args) throws InterruptedException {
        // 初始化数据流和算子
        DataStream<String> sourceStream = new DataStream<>();
        DataStream<String> mappedStream = new DataStream<>();

        Source source = new Source(sourceStream, "input-topic");
        MapOperator<String, String> map = new MapOperator<>(
                sourceStream,
                mappedStream,
                s -> "Processed: " + s.toUpperCase()  // 示例处理逻辑：添加前缀并转大写
        );

        Sink sink = new Sink(mappedStream, "output.txt");

        // 启动线程
        Thread sourceThread = new Thread(source);
        Thread mapThread = new Thread(map);
        Thread sinkThread = new Thread(sink);
        sourceThread.start();
        mapThread.start();
        sinkThread.start();

        // 运行 60 秒后停止
        Thread.sleep(60_000);
        source.stop();
        map.stop();
        sink.stop();

        // 等待线程结束
        sourceThread.join();
        mapThread.join();
        sinkThread.join();
    }
}