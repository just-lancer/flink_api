package case_.operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author shaco
 * @create 2023-03-03 10:54
 * @desc 从集合中读取数据
 */
public class C001_ReadMemorySource {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置全局并行度为1，便于控制台数据打印
        env.setParallelism(1);

        // TODO 2、创建集合，作为数据源
        ArrayList<String> arrayListSource = new ArrayList<>();
        arrayListSource.add("hello world");
        arrayListSource.add("hello java");
        arrayListSource.add("hello scala");
        arrayListSource.add("hello python");
        arrayListSource.add("hello shell");
        arrayListSource.add("hello flink");
        arrayListSource.add("hello spark");

        // TODO 3、从集合中读取数据源
        DataStreamSource<String> stringDataStreamSource = env.fromCollection(arrayListSource);

        // TODO 4、直接在控制台打印数据源
        stringDataStreamSource.print();

        // TODO 5、执行流式数据处理
        env.execute();
    }
}
