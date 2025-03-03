package DAG.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OperatorConfig {
    private String id;
    private String type;
    private String function;
    @JsonProperty("key-selector")
    private String keySelector;
    private int parallelism;
    private String next;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }

    public String getKeySelector() {
        return keySelector;
    }

    public void setKeySelector(String keySelector) {
        this.keySelector = keySelector;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public String getNext() {
        return next;
    }

    public void setNext(String next) {
        this.next = next;
    }
}
