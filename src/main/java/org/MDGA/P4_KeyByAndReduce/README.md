# 简易流计算系统设计

## 项目概述
本项目实现了一个简易的流计算系统，支持KeyBy和Reduce算子，并提供了基于处理时间的滚动窗口和并发处理功能。系统设计采用了DAG（有向无环图）形式来编排算子，使用者可以灵活配置算子的上下游关系。

### 主要特性
- 支持数据流的创建和基本操作
- 提供KeyBy算子实现数据分组
- 支持Reduce算子进行数据归约
- 基于处理时间的滚动窗口机制
- 可配置的并行处理能力

### 适用场景
- 实时数据处理和分析
- 流式计算场景
- 数据分组统计
- 并行数据处理

## 系统架构

### 核心接口
1. `DataStream<T>`：基础数据流接口
   - `keyBy`：按key分组
   - `setParallelism`：设置并行度
   - `withWindow`：设置窗口大小

2. `KeyedDataStream<T, K>`：分组后的数据流接口
   - `reduce`：归约操作
   - `getKey`：获取分组key

### 实现类
1. `BaseDataStream<T>`：基础数据流实现
   - 维护数据列表
   - 管理并行度和窗口配置

2. `KeyedDataStreamImpl<T, K>`：分组数据流实现
   - 实现数据分组逻辑
   - 执行归约操作

## 功能特性

### 1. KeyBy算子
- 功能：将数据流按照指定的key分割成多个正交的数据流
- 实现：通过函数式接口提取key，使用Java Stream API进行分组

### 2. Reduce算子
- 功能：对分组后的数据进行自定义归约操作
- 实现：支持任意二元归约函数，对每个分组独立进行归约

### 3. 窗口机制
- 类型：基于处理时间的滚动窗口
- 配置：通过withWindow方法设置窗口大小（毫秒）

### 4. 并行处理
- 功能：支持为算子指定并行度
- 配置：通过setParallelism方法设置

## 快速开始

### 环境要求
- JDK 8 或更高版本（本人开发的环境是JDK 23）
- Java开发IDE（推荐使用IntelliJ IDEA或Eclipse）

### 基本使用步骤
1. 创建数据流
2. 配置流处理参数（并行度、窗口大小）
3. 使用算子进行数据处理
4. 获取处理结果

### 代码示例
```java
// 1. 创建数据流
List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
DataStream<Integer> dataStream = new BaseDataStream<>(numbers);

// 2. 配置并行度和窗口
dataStream = dataStream.setParallelism(2)  // 设置2个并行线程
           .withWindow(1000);              // 设置1秒的处理窗口

// 3. 使用KeyBy和Reduce算子
DataStream<Integer> result = dataStream
    .keyBy(num -> num % 2 == 0 ? "偶数" : "奇数")  // 按奇偶性分组
    .reduce(Integer::sum);                          // 分组求和

// 4. 获取结果
((BaseDataStream<Integer>)result).getData()
    .forEach(sum -> System.out.println("分组求和结果: " + sum));
```

### 更多示例
1. 字符串长度分组统计
```java
List<String> words = Arrays.asList("hello", "world", "stream", "processing");
DataStream<String> wordStream = new BaseDataStream<>(words);

DataStream<Integer> result = wordStream
    .keyBy(word -> String.valueOf(word.length()))  // 按长度分组
    .reduce((w1, w2) -> w1 + ", " + w2);         // 连接同长度的字符串
```

2. 数值范围分组
```java
List<Double> values = Arrays.asList(1.2, 3.4, 5.6, 7.8, 9.0);
DataStream<Double> valueStream = new BaseDataStream<>(values);

DataStream<Double> result = valueStream
    .keyBy(val -> val < 5.0 ? "低" : "高")  // 按数值范围分组
    .reduce(Double::sum);                    // 分组求和
```

## 扩展性设计
1. 接口抽象：通过接口定义实现解耦，便于扩展新的算子
2. 泛型支持：支持处理任意类型的数据
3. 函数式接口：支持自定义key提取和归约逻辑

## 注意事项
1. 窗口大小必须大于0
2. 并行度必须大于0
3. 在使用reduce操作前必须先进行keyBy分组
4. 获取最终结果时需要转换为BaseDataStream类型

## Kafka集成指南

### Kafka配置
1. 服务器配置
   ```java
   String bootstrapServers = "localhost:9092";
   String inputTopic = "input-topic";
   String outputTopic = "output-topic";
   String groupId = "stream-processor-group";
   ```

### 数据源配置（KafkaSource）
1. 创建数据源
   ```java
   KafkaSource<Integer> source = new KafkaSource<>(
       bootstrapServers,
       inputTopic,
       groupId,
       Integer::parseInt,  // 值反序列化函数
       100                // 批处理大小
   );
   ```

2. 主要参数说明
   - `bootstrapServers`: Kafka服务器地址
   - `topic`: 订阅的主题
   - `groupId`: 消费者组ID
   - `valueDeserializer`: 值反序列化函数
   - `batchSize`: 批处理大小

3. 配置项
   - 自动提交：earliest
   - 反序列化：StringDeserializer
   - 批处理：支持自定义批次大小

### 输出配置（KafkaSink）
1. 创建输出目标
   ```java
   KafkaSink<Integer> sink = new KafkaSink<>(
       bootstrapServers,
       outputTopic,
       String::valueOf  // 值序列化函数
   );
   ```

2. 主要参数说明
   - `bootstrapServers`: Kafka服务器地址
   - `topic`: 目标主题
   - `valueSerializer`: 值序列化函数

3. 配置项
   - 序列化：StringSerializer
   - 自动刷新：支持手动和自动刷新

### 完整示例
```java
// 1. 创建数据源和数据流
KafkaSource<Integer> source = new KafkaSource<>(
    bootstrapServers,
    inputTopic,
    groupId,
    Integer::parseInt,
    100
);

// 2. 配置数据流处理
DataStream<Integer> dataStream = source.getDataStream()
    .setParallelism(2)
    .withWindow(1000);

// 3. 使用KeyBy和Reduce进行处理
DataStream<Integer> result = dataStream
    .keyBy(num -> num % 2 == 0 ? "偶数" : "奇数")
    .reduce(Integer::sum);

// 4. 配置输出并写入结果
KafkaSink<Integer> sink = new KafkaSink<>(
    bootstrapServers,
    outputTopic,
    String::valueOf
);

try {
    result.getData().forEach(sink::write);
} finally {
    source.close();
    sink.close();
}
```

### 注意事项
1. 资源管理
   - 及时关闭KafkaSource和KafkaSink
   - 使用try-finally确保资源正确释放

2. 数据序列化
   - 输入数据：字符串格式，需要通过valueDeserializer转换
   - 输出数据：需要通过valueSerializer转换为字符串

3. 性能优化
   - 合理设置批处理大小
   - 根据数据量配置合适的并行度
   - 设置适当的窗口大小