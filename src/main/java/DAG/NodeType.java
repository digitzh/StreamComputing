package DAG;

/**
 * 节点类型枚举类
 * 定义DAG中节点的类型：数据源(SOURCE)、操作算子(OPERATOR)和数据接收器(SINK)
 */
public enum NodeType {
    SOURCE("source"),
    MAP("map"),
    KEYBY("keyby"),
    REDUCE("reduce"),// 数据源节点
    SINK("sink");        // 数据接收器节点
    
    private final String value;
    
    /**
     * 构造函数
     * @param value 节点类型的字符串表示
     */
    NodeType(String value) {
        this.value = value;
    }
    
    /**
     * 获取节点类型的字符串表示
     * @return 节点类型字符串
     */
    public String getValue() {
        return value;
    }
    
    /**
     * 从字符串解析节点类型
     * @param text 要解析的字符串
     * @return 对应的NodeType枚举值
     * @throws IllegalArgumentException 当找不到匹配的节点类型时抛出
     */
    public static NodeType fromString(String text) {
        for (NodeType type : NodeType.values()) {
            if (type.value.equalsIgnoreCase(text)) {
                return type;
            }
        }
        throw new IllegalArgumentException("未知的节点类型: " + text);
    }
} 