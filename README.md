# StreamComputing

Let's **M**ake **D**ata **G**reat **A**gain!

## 1 基本模块介绍

src/main/java目录定义了StreamComputing的核心模块。

### 1.1 基本数据结构

- DataStream：数据流结构，用于表示数据流的输入和输出。
- DataGenerator：数据生成器，用于生成数据流的输入数据。
- StreamContext：流上下文，用于管理流的执行环境和配置。
- StreamOperator：流算子，用于定义流的处理逻辑，如设置并行度等。
- StreamRecord：流记录，用于表示流中的数据记录。
- OperatorFactory：操作符工厂，用于创建流操作符的实例。
- KeyedDataStream：按键分区流，用于按键分区流的操作。

### 1.2 算子

- Source：数据源，用于从外部存储中读取数据流的输入数据。
- MapOperator：映射算子，用于映射流中的数据。
- KeyByOperator：KeyBy算子，用于根据Key分区。
- ReduceOperator：归约算子，用于归约流。
- FileSink：文件数据汇，用于将流的输出数据写入外部存储。
- KafkaSink：Kafka数据汇，用于将流的输出数据写入Kafka。

- WindowOperator：窗口算子，用于定义窗口的处理逻辑。
- ShuffleOperator：Shuffle算子，用于节点数据Shuffle。

### 1.3 DAG编排

定义了DAG节点、解析器、调度器等，用于构建和执行流的DAG。
dag-config.yaml：DAG配置文件，用于定义流的DAG。

### 1.4 其他模块

- SetParallelism：设置并行度，用于设置流的并行度。
- WordCountEvent：单词计数事件，用于统计单词出现次数。

## 2 部署启动项目

### 2.1 配置Kafka

在任意节点启动Kafka。在KafkaConfig.java中配置Kafka的地址和端口：
```java
    // 修改为本地开发环境的Kafka服务器地址和端口
    public static String IP_PORT = "192.168.233.129:9092";
```

### 2.2 启动Kafka producer和consumer

在任意节点启动Kafka producer和consumer。配置producer的topic为`input-topic`，consumer的topic为`output-topic`。示例命令：
```shell
# 启动Kafka producer
./bin/kafka-console-producer.sh --broker-list node1:9092 --topic input-topic
# 在另一终端启动Kafka consumer
./bin/kafka-console-consumer.sh --bootstrap-server node1:9092 --topic output-topic
```

### 2.3 启动项目

所有测试程序都在src/test/java目录下，可以直接运行。

| 测试程序                     | 功能             | producer topic 设置      | consumer topic 设置   |
|--------------------------|----------------|------------------------|---------------------|
| TestBasicOperator.java   | 测试基本算子         | input-topic            | output-topic        |
| TestParallelism.java     | 测试并行度          | 无需设置(源使用DataGenerator) | output-topic        |
| TestShuffle.java         | 测试Shuffle      | 无需设置(源使用DataGenerator) | output-topic        |
| TestWordCount.java       | 测试单词计数         | 无需设置(源使用DataGenerator) | output-topic        |
| TestWordCountConfig.java | 测试单词计数(配置文件编排) | 无需设置(源使用DataGenerator) | 无需设置(输出到output.txt) |

除了第1项需要运行producer并设置topic外，其他程序无需设置producer；且所有程序都可以不设置consumer，而是在控制台或文件查看输出。

第1项程序运行后，需要在控制台输入单词及出现次数，格式为`<word>,<count>`，如`hello,1`，并查看输出。
其他程序只需运行后查看输出即可。

## 3 验证该项目

| 测试程序                     | 功能             | 验证点                                   |
|--------------------------|----------------|---------------------------------------|
| TestBasicOperator.java   | 测试基本算子         | 查看输入是否能正常输出，实现WordCount               |
| TestParallelism.java     | 测试并行度          | 查看多并行度下程序是否能正常运行                      |
| TestShuffle.java         | 测试Shuffle      | 查看控制台Shuffle后的统计信息                    |
| TestWordCount.java       | 测试单词计数         | 查看控制台和Kafka consumer，验证是否能实现WordCount |
| TestWordCountConfig.java | 测试单词计数(配置文件编排) | 查看控制台和output.txt，验证是否能实现WordCount     |

详情请参考项目文档及视频：
[MDGA简易流计算系统项目文档](https://wcnedza3fzle.feishu.cn/wiki/P04Pw9DJHi9cMIkaNtBcLCpOnLg)
