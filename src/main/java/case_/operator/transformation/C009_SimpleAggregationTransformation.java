package case_.operator.transformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shaco
 * @create 2023-03-09 11:39
 * @desc 简单聚合函数
 */
public class C009_SimpleAggregationTransformation {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、创建数据源，直接枚举数据
        DataStreamSource<Tuple3<String, Integer, Integer>> aggregationDS = env.fromElements(
                Tuple3.of("a", 1, 10000),
                Tuple3.of("a", 4, 400),
                Tuple3.of("a", 4, 500),
                Tuple3.of("a", 3, 300)
        );

        // TODO 3、按用户名称进行分组
        KeyedStream<Tuple3<String, Integer, Integer>, String> keyedDS = aggregationDS.keyBy(
                new KeySelector<Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );

        // TODO 5、聚合，并在控制台打印
        keyedDS.max("f1").print("~~~~");
        keyedDS.maxBy("f1").print("====");

        // TODO 6、执行流数据处理
        env.execute();
    }
}
