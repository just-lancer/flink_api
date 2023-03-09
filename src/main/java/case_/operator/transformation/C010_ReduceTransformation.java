package case_.operator.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

/**
 * @author shaco
 * @create 2023-03-09 16:46
 * @desc 归约聚合reduce，需求：使用reduce实现maxBy()，求当前访问量最大的用户
 */
public class C010_ReduceTransformation {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、创建数据源，使用流式数据
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、数据处理逻辑
        // 为提高数据传输效率，过滤掉无效的字段
        SingleOutputStreamOperator<Tuple2<String, Long>> mapDS = webPageAccessEventDS.map(
                new MapFunction<WebPageAccessEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(WebPageAccessEvent value) throws Exception {
                        return Tuple2.of(value.userName, 1L);
                    }
                }
        );

        // 统计出每个用户的访问量，先分组，然后计算每个用户的访问量
        SingleOutputStreamOperator<Tuple2<String, Long>> accessAmountDS = mapDS.keyBy(
                new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                }
        ).reduce(
                new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }
        );

        // 再将各个用户的统计数据放到一起进行比较，求出访问量最大的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> maxAccessAmountDS = accessAmountDS.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                // 这里指定常量，目的是将所有用户的数据分到同一个组中
                return "userGroup";
            }
        }).reduce(
                new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                }
        );

        // TODO 4、控制台打印
        maxAccessAmountDS.print();

        // TODO 5、执行流数据处理
        env.execute();
    }
}