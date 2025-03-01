
//import DataStream;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 将数据写入文件的 Sink 算子
 */
public class Sink implements Runnable {
    private final DataStream<String> dataStream;
    private final String outputPath;
    private volatile boolean isRunning = true;

    public Sink(DataStream<String> dataStream, String outputPath) {
        this.dataStream = dataStream;
        this.outputPath = outputPath;
    }

    @Override
    public void run() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath, true))) {
            while (isRunning) {
                String record = dataStream.poll();
                writer.write(record);
                writer.newLine();
                writer.flush(); // 立即写入磁盘
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        isRunning = false;
    }
}