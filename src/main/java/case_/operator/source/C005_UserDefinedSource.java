package case_.operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

/**
 * @author shaco
 * @create 2023-03-03 15:27
 * @desc 读取用户自定义数据源
 */
public class C005_UserDefinedSource {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取自定义数据源
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、打印数据流
        webPageAccessEventDS.print();

        // TODO 4、执行流数据处理
        env.execute();
    }
}
