package case_.operator.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import pojoclass.WebPageAccessEvent;

/**
 * @author shaco
 * @create 2023-03-09 11:00
 * @desc flatMap算子。需求，过滤掉Bob用户的数据，Anna用户的数据不做任何处理，直接发送下游，Carter用户的数据，删除其操作时间
 */
public class C008_FlatMapTransformation {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、创建数据源，直接枚举数据
        DataStreamSource<WebPageAccessEvent> sampleDS = env.fromElements(
                new WebPageAccessEvent("Anna", "./start", "1000"),
                new WebPageAccessEvent("Bob", "./market", "2000"),
                new WebPageAccessEvent("Carter", "./advertising", "3000")
        );

        // TODO 3、调用flatMap()方法，过滤掉Bob用户的数据，Anna用户的数据不做任何处理，直接发送下游，Carter用户的数据，删除其操作时间
        SingleOutputStreamOperator<String> flatMapDB = sampleDS.flatMap(
                new FlatMapFunction<WebPageAccessEvent, String>() {
                    @Override
                    public void flatMap(WebPageAccessEvent value, Collector<String> out) throws Exception {
                        if ("Anna".equals(value.userName)) {
                            out.collect(value.toString());
                        } else if ("Carter".equals(value.userName)) {
                            out.collect(value.userName + ": " + value.url);
                        }
                    }
                }
        );

        // TODO 4、在控制台打印流数据
        flatMapDB.print();

        // TODO 5、执行流数据处理
        env.execute();
    }
}