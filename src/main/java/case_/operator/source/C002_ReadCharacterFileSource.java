package case_.operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shaco
 * @create 2023-03-03 13:22
 * @desc 读取文本文件作为数据源
 */
public class C002_ReadCharacterFileSource {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取文本文件
        // 读取本地文本文件
        DataStreamSource<String> stringDSLocal = env.readTextFile("C:\\my_workplace_git\\flink_api\\input\\text2_world.txt");
        // 读取文件系统中的文本文件
        DataStreamSource<String> stringDSFileSystem = env.readTextFile("hdfs://hadoop132:8020/flink_input/text2_world.txt");

        // TODO 3、打印数据流
        stringDSLocal.print(">>>>");
        stringDSFileSystem.print("====");

        // TODO 4、执行流数据处理
        env.execute();
    }
}
