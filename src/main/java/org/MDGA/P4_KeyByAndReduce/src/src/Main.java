package src;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import stream.BaseDataStream;
import stream.DataStream;

/**
 * 简易流计算系统使用示例
 * 本示例展示了如何使用系统的核心功能：
 * 1. 创建数据流
 * 2. 配置并行度和窗口
 * 3. 使用KeyBy进行数据分组
 * 4. 使用Reduce进行数据归约
 */
public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        // 步骤1：创建示例数据
        // 这里我们创建一个包含1到10的整数列表作为输入数据
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 步骤2：创建基础数据流
        // BaseDataStream是DataStream接口的基本实现，用于封装数据列表
        DataStream<Integer> dataStream = new BaseDataStream<>(numbers);

        // 步骤3：配置数据流属性
        // setParallelism：设置并行处理的线程数，这里设置为2表示使用2个线程处理数据
        // withWindow：设置窗口大小（毫秒），这里设置为1000ms表示每秒进行一次计算
        dataStream = dataStream.setParallelism(2).withWindow(1000);

        // 步骤4：使用KeyBy和Reduce算子处理数据
        // keyBy：按照奇偶性对数据进行分组，返回KeyedDataStream
        // reduce：对每个分组的数据进行求和操作
        DataStream<Integer> result = dataStream
                .keyBy(num -> num % 2 == 0 ? "偶数" : "奇数") // 提取分组key的函数
                .reduce(Integer::sum); // 归约函数，这里使用Integer类的sum方法

        // 步骤5：获取和输出结果
        // 注意：需要将结果流转换回BaseDataStream才能获取数据
        // 最终会输出两个结果：奇数之和和偶数之和
        ((BaseDataStream<Integer>)result).getData().forEach(sum -> 
            logger.info("分组求和结果: " + sum));
    }
}