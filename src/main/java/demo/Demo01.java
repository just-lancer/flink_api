package demo;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shaco
 * @create 2023-03-02 18:07
 * @desc
 */
public class Demo01 {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、利用fromElement()方法，读取数据源
        DataStreamSource<String> stringDataStreamSource = env.fromElements(
                "hello world",
                "hello java",
                "hello scala",
                "hello python",
                "hello flink"
        );

        // TODO 3、控制台打印数据流
        stringDataStreamSource.print();

        // TODO 4、执行流数据处理
        env.execute();
    }
}