package demo;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;
import utils.CustomerTimeUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * @author shaco
 * @version Flink 1.13.6，Flink CDC 2.2.1
 * @create 2023-05-23 17:06
 * @desc 入门案例，每个用户的UV统计，并将用户信息写入MySQL
 */
public class Demo07_Introduction {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 将全局并行度设置为 1
        env.setParallelism(1);

        // TODO 2、读取数据源，自定义的实时流数据
        DataStreamSource<WebPageAccessEvent> unboundedDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、业务逻辑的处理
        SingleOutputStreamOperator<String> mapDS = unboundedDS
                .keyBy(ele -> ele.userName)
                .map(
                        new RichMapFunction<WebPageAccessEvent, String>() {
                            // 声明一个值状态，相当于Java里的一个变量
                            MapState<String, Integer> uvCount;
                            // 声明一个数据库连接
                            Connection connection;
                            PreparedStatement ps;

                            // 第一条数据到来之前，调用该方法
                            @Override

                            public void open(Configuration parameters) {
                                // 初始化状态
                                // 状态描述器
                                MapStateDescriptor<String, Integer> uvCountDesc = new MapStateDescriptor<>("uvCountDesc", String.class, Integer.class);
                                uvCount = getRuntimeContext().getMapState(uvCountDesc);

                                // 开启JDBC连接
                                try {
                                    Class.forName("com.mysql.cj.jdbc.Driver");
                                    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?serverTimezone=UTC", "root", "1234");
                                    ps = connection.prepareStatement("insert into useraccessevent values (?,?)");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }

                            // 最后一条数据计算完成后，调用该方法，对于流式数据，当任务终止的时候会调用该方法
                            @Override
                            public void close() throws Exception {
                                // 关闭JDBC连接
                                if (connection != null) {
                                    try {
                                        connection.close();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }

                                if (ps != null) {
                                    try {
                                        ps.close();
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }

                            }

                            @Override
                            public String map(WebPageAccessEvent value) throws Exception {
                                // 业务逻辑处理 1：统计用户UV数据
                                // 如果是用户的第一条访问数据
                                if (!uvCount.contains(value.userName)) {
                                    uvCount.put(value.userName, 1);
                                } else {
                                    Integer uv = uvCount.get(value.userName);
                                    uvCount.put(value.userName, ++uv);
                                }

                                // 业务逻辑处理 2：将用户信息写入MySQL数据库
                                ps.setString(1, value.userName);
                                ps.setTimestamp(2, new Timestamp(CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss")));

                                ps.executeUpdate();

                                return "(" + value.userName + ": " + uvCount.get(value.userName) + ")";
                            }
                        }
                );

        // TODO 执行流数据处理，并将UV统计结果打印到控制台
        unboundedDS.print();
        mapDS.print(">>>>");
        env.execute();
    }
}
