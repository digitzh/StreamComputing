import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WindowOperator implements Runnable {
    private final DataStream<WordCountEvent> inputStream;
    private final DataStream<String> outputStream;
    private volatile boolean isRunning = true;
    private final long windowSizeMillis;

    public WindowOperator(DataStream<WordCountEvent> inputStream, DataStream<String> outputStream, long windowSizeMillis) {
        this.inputStream = inputStream;
        this.outputStream = outputStream;
        this.windowSizeMillis = windowSizeMillis;
    }

    @Override
    public void run() {
        // 计算下一个窗口结束时间（对齐到 windowSizeMillis 的倍数）
        long currentTime = System.currentTimeMillis();
        long windowEnd = ((currentTime / windowSizeMillis) + 1) * windowSizeMillis;
        Map<String, Integer> aggregator = new HashMap<>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd'T'HH:mm:ss");

        while (isRunning) {
            long timeout = windowEnd - System.currentTimeMillis();
            if(timeout < 0) {
                timeout = 0;
            }
            try {
                // 尝试在当前窗口内等待数据
                WordCountEvent event = inputStream.poll(timeout, TimeUnit.MILLISECONDS);
                if (event != null) {
                    // 将单词转换为小写（尽管在 MapOperator 里已经转换，但这里再保证一次）
                    String word = event.word.toLowerCase();
                    aggregator.put(word, aggregator.getOrDefault(word, 0) + event.count);
                } else {
                    // 超时，说明当前窗口结束，输出聚合结果
                    String triggerTime = sdf.format(new Date(windowEnd));
                    for (Map.Entry<String, Integer> entry : aggregator.entrySet()) {
                        String output = triggerTime + "," + entry.getKey() + "," + entry.getValue();
                        outputStream.emit(output);
                        System.out.println("[WindowOperator] Emitted: " + output);
                    }
                    // 清空聚合器，更新下一个窗口结束时间
                    aggregator.clear();
                    windowEnd += windowSizeMillis;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void stop() {
        isRunning = false;
    }
}
