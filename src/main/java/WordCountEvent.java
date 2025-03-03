public class WordCountEvent {
    public String word;
    public int count;
    public long timestamp; // 当前处理时间

    public WordCountEvent(String word, int count, long timestamp) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
    }
}
