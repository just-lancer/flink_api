package case_.operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shaco
 * @create 2023-03-03 14:47
 * @desc 读取socket文本流
 */
public class C003_ReadSocketTextSource {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取Socket文本流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop132", 9999);

        // TODO 3、打印数据流
        socketTextStream.print();

        // TODO 4、执行流数据处理
        env.execute();
    }
}
