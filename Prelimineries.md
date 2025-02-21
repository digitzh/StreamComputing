## 1 预备工作
### 1.1 配置
开始之前，需要下载好[VMWare 17](https://pan.baidu.com/s/1QlOmIrlM_x9HwdaTDf-oCw?pwd=9tj3)及对应的[Ubuntu 22.04](https://releases.ubuntu.com/22.04/ubuntu-22.04.5-desktop-amd64.iso.torrent)镜像，以及[MobaExterm](https://mobaxterm.mobatek.net/download-home-edition.html)。
1. 配置大数据统一环境（见“Env(1) 大数据环境统一配置”）
2. 配置Zookeeper（见“Env(4) Zookeeper集群安装”）
3. 配置Kafka（见“Env(5) Kafka环境搭建”）
4. 安装IDEA(下载[IDEA 2024.1](https://download.jetbrains.com/idea/ideaIU-2024.1.exe)，便于激活。JetBrains激活资源： [链接点此](https://pan.baidu.com/s/14m3YF1sidQIBV5Zrb_FISg?pwd=2kic) ，提取码: 2kic)

以上提到的“黑马环境配置文档”[链接点此](https://pan.baidu.com/s/1xWftWOZEpPF82NoHXMdhvQ?pwd=5735)，提取码: 5735 
### 1.2 学习

1. 学习Java。Java的基础语法和C++类似。关注一下流的数据结构（IntStream）及方法（map）等。关于接口及其实现，特别是匿名内部类，Lambda表达式等，比较重要，可以在第2条（学习Flink）的同时逐渐熟悉。我建议我们使用Java 17，这是一个支持Lambda表达式的LTS版本。
2. 学习Flink。推荐[尚硅谷Flink视频](https://www.bilibili.com/video/BV1eg4y1V7AN/)。重点：（1）如何在IDEA中创建Maven工程和导入依赖，自己创建一个Demo，运行和调试。（2）流处理流程，以WordCount为例。（3）视频的DataStream API部分，各算子的效果。项目要求的都是重点，从Kafka读取(fromSource)、输出到文件(SinkTo)等。了解pom.xml的配置方法，会利用DataStream API实现功能。如果有时间，可以跟着视频把DataStream的Demo敲一遍。（4）注意搭配笔记（百度网盘[链接点此](https://pan.baidu.com/s/1tCHNtxe1H44CHStPAO-KQA?pwd=qyvr)，提取码: qyvr）。
3. 后面还有如窗口、水位线、精准一次等，但我们最好先满足项目的基础要求。我们需要阅读DataStream的源码，学习它每个算子的实现方式，实现了哪些接口，逻辑流程是怎样的。尽量在不引入Flink依赖的情况下仿写DataStream API，写出一个最小的流计算系统。
4. 其他技术基础，比如Git的使用（用于合作完成项目，如add, commit, status, push, pull, stash, pop等）、Linux的基本操作（操作虚拟机，如mv, scp, apt, mkdir, sudo, chmod, ifconfig，使用vim，运行脚本等）、markdown（便于写文档）等。
我目前的思路是，当大家都打好上面的基础后，针对项目基本要求中的5个算子分工，实现第3点。解耦是很关键的一点，从0开始构建最小的简易流系统，在阅读源码之中，只保留核心的实现逻辑。这些只是我的想法，具体后面我们再讨论。
### 1.3 典型DataStream API使用示例

下面是尚硅谷视频的KafkaSourceDemo。
```java
public static void main(String[] args) throws Exception {  
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  
  
    env.setParallelism(1);
  
    KafkaSource<String> kafkaSource = KafkaSource.<String>builder()  
            .setBootstrapServers("node1:9092,node2:9092,node3:9092")
            .setGroupId("org.example")
            .setTopics("topic_1") 
            .setValueOnlyDeserializer(new SimpleStringSchema()) 
            .setStartingOffsets(OffsetsInitializer.earliest())
            .build();  
  
    // 3个参数: fileSource实例, Watermark, source名称  
    env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkasource")  
            .print();  
  
    env.execute();  
}
```
### 1.4 结语

首先，相信自己。我们相聚于此，也许大家在自己的技术栈各有优势，但既然来学习分布式计算、Flink等字节课程，总体来说还是之前不熟悉，假期通过学习逐渐有所了解，最后来做此项目。至少在项目方面，大家是从起步到熟悉的逐渐过渡，诚宜坦诚交流，不宜妄自菲薄。小白没关系，只要花功夫，庶几终有成。

其次，关于我们所能采取的主动态度。基于上述原因，我建议抱团取暖。我本硕是通信专业，硕士偏计算机，但寒假之前语言一直是C/C++。坦诚地说，我在寒假之前，对大数据知之甚少。经过学习，现在我对Java和Flink有所了解，但对于项目的想法，仍然是初步的。我们团队中有不少大数据/AI科班的同学，有Java全栈的同学。大家无疑可以有比我更高的自信和主动，探讨和决定项目的走向。主要是鼓励大家多多交流啦。

尽管从完成项目的角度来说，我们可能需要分工，但从深入理解整个项目、从而自信地将其写在简历上的角度来说，熟悉整个项目对每个人来说是必要的。大学课程作业，可能多做少做，最后评分一样；但我们做项目，多做一分有一分的长进，有一分项目写在简历上的底气。打铁还需自身硬，纵使划水，将来终要补齐，以应对面试官的发问，不妨现在团结起来，同舟共济。所以我们合作的重心，还是增长知识，学习技术。

祝我们合作顺利，祝各位学有所成，加油！