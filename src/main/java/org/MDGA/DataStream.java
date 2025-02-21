package org.MDGA;

import java.util.ArrayList;

public class DataStream<T> {
    /*
    // 数据源或上游算子
    private final StreamSource<T> source;
    // 当前算子的处理逻辑链
    private final List<StreamOperator<T>> operators;

    // 构造函数（通常不直接暴露给用户）
    protected DataStream(StreamSource<T> source, List<StreamOperator<T>> operators) {
        this.source  = source;
        this.operators  = operators;
    }

    // 链式调用：添加新算子并生成新DataStream实例
    public <R> DataStream<R> applyOperator(StreamOperator<R> operator) {
        List<StreamOperator<?>> newOperators = new ArrayList<>(this.operators);
        newOperators.add(operator);
        return new DataStream<>(this.source,  newOperators);
    }
    */
}
