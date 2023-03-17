package demo;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Random;

/**
 * @author shaco
 * @create 2023-03-10 16:12
 * @desc 用户自定义分区
 */
public class Demo02 {
    public static void main(String[] args) throws Exception {
        // 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2、读取数据源
        DataStreamSource<String> stringDataStreamSource = env.fromElements(
                "hello world",
                "hello java",
                "hello scala",
                "hello python",
                "hello flink"
        );

        // 3、为每条数据附上一个key
        SingleOutputStreamOperator<Tuple2<Integer, String>> mapDS = stringDataStreamSource.map(
                new RichMapFunction<String, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> map(String value) throws Exception {
                        Tuple2 data = Tuple2.of(new Random().nextInt(10), value);
                        return data;
                    }
                }
        );

        // 4、进行自定义分区
        mapDS.partitionCustom(
                new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        // numPartitions是默认的分区数量，取值为CPU的核心数量
                        return key;
                    }
                },
                new KeySelector<Tuple2<Integer, String>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                        return value.f0;
                    }
                }
        ).print(">>>>");

        // 5、执行流数据处理
        env.execute();
    }
}
