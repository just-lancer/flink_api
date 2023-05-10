package case_.Flink_SQL;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

/**
 * @author shaco
 * @create 2023-05-09 16:45
 * @desc 基于表执行环境，创建表
 */
public class C030_CreateTable {
    public static void main(String[] args) {
        // TODO 创建流执行环境和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建数据流
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 将数据流转换成表
        Table table = tableEnv.fromDataStream(webPageAccessEventDS);

        // 在表执行环境中注册表
        tableEnv.createTemporaryView("temp_tb", table);

        // 调用SQl查询
        TableResult tableResult = tableEnv.executeSql("insert into temp_tb ('bob', './home', '2023-05-09 23:00:00')");


    }
}
