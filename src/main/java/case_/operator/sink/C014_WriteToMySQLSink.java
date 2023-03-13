package case_.operator.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author shaco
 * @create 2023-03-13 19:25
 * @desc Sink算子演示，写入到MySQL
 */
public class C014_WriteToMySQLSink {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、获取数据源
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.fromElements(
                new WebPageAccessEvent("Anna", "./home", "1000"),
                new WebPageAccessEvent("Bob", "./favor", "2000")
        );

        // TODO 3、将数据写入到MySQL中
        webPageAccessEventDS.addSink(
                JdbcSink.sink(
                        "INSERT INTO demo (user, url) VALUES (?, ?)",
                        new JdbcStatementBuilder<WebPageAccessEvent>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, WebPageAccessEvent webPageAccessEvent) throws SQLException {
                                preparedStatement.setString(1, webPageAccessEvent.userName);
                                preparedStatement.setString(2, webPageAccessEvent.url);
                            }
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://localhost:3306/test")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .withUsername("root")
                                .withPassword("1234")
                                .build()
                )
        );

        // TODO 4、执行流数据处理
        env.execute();
    }
}
