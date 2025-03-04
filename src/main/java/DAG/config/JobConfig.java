package DAG.config;

import java.util.List;

/**
 * 作业配置类
 * 定义一个数据处理作业的整体配置，包括数据源、操作算子和数据接收器
 */
public class JobConfig {
    private String name;                   // 作业名称
    private List<SourceConfig> sources;    // 数据源配置列表
    private List<OperatorConfig> operators; // 操作算子配置列表
    private List<SinkConfig> sinks;        // 数据接收器配置列表

    /**
     * 获取作业名称
     * @return 作业名称
     */
    public String getName() {
        return name;
    }

    /**
     * 设置作业名称
     * @param name 作业名称
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 获取数据源配置列表
     * @return 数据源配置列表
     */
    public List<SourceConfig> getSources() {
        return sources;
    }

    /**
     * 设置数据源配置列表
     * @param sources 数据源配置列表
     */
    public void setSources(List<SourceConfig> sources) {
        this.sources = sources;
    }

    /**
     * 获取操作算子配置列表
     * @return 操作算子配置列表
     */
    public List<OperatorConfig> getOperators() {
        return operators;
    }

    /**
     * 设置操作算子配置列表
     * @param operators 操作算子配置列表
     */
    public void setOperators(List<OperatorConfig> operators) {
        this.operators = operators;
    }

    /**
     * 获取数据接收器配置列表
     * @return 数据接收器配置列表
     */
    public List<SinkConfig> getSinks() {
        return sinks;
    }

    /**
     * 设置数据接收器配置列表
     * @param sinks 数据接收器配置列表
     */
    public void setSinks(List<SinkConfig> sinks) {
        this.sinks = sinks;
    }
}
