

import DataStream;
import Sink;
import Source;

public class StreamingJob {
    public static void main(String[] args) throws InterruptedException {
        // 初始化数据流和算子
        DataStream<String> dataStream = new DataStream<>();
        Source source = new Source(dataStream, "input-topic");
        Sink sink = new Sink(dataStream, "output.txt");

        // 启动线程
        Thread sourceThread = new Thread(source);
        Thread sinkThread = new Thread(sink);
        sourceThread.start();
        sinkThread.start();

        // 运行 60 秒后停止
        Thread.sleep(60_000);
        source.stop();
        sink.stop();

        // 等待线程结束
        sourceThread.join();
        sinkThread.join();
    }
}