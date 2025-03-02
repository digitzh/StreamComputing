package DAG.config;

import java.util.List;

public class JobConfig {
    private String name;
    private List<SourceConfig> sources;
    private List<OperatorConfig> operators;
    private List<SinkConfig> sinks;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<SourceConfig> getSources() {
        return sources;
    }

    public void setSources(List<SourceConfig> sources) {
        this.sources = sources;
    }

    public List<OperatorConfig> getOperators() {
        return operators;
    }

    public void setOperators(List<OperatorConfig> operators) {
        this.operators = operators;
    }

    public List<SinkConfig> getSinks() {
        return sinks;
    }

    public void setSinks(List<SinkConfig> sinks) {
        this.sinks = sinks;
    }
}
