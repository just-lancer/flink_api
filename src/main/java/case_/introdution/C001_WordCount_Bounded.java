package case_.introdution;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author shaco
 * @create 2023-03-02 17:51
 * @desc 入门案例：有界流的World Count
 */
public class C001_WordCount_Bounded {
    public static void main(String[] args) throws Exception{
        // TODO 1、创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 2、读取文件
        // 以下方法是按行读取文件的
        DataStreamSource<String> inputDS = env.readTextFile("input\\text1_world.txt");

        // TODO 3、对读取的每一行数据进行拆分，拆分成不同的单词
        SingleOutputStreamOperator<Tuple2<String, Integer>> worldOfOne = inputDS.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        // 单词分解
                        // 需要注意的是，数据是一行一行读取，并被处理的
                        String[] worldList = value.split(" ");

                        // 遍历单词数组，并添加到Tuple中
                        for (String world : worldList) {
                            // 将(world,1)输出
                            out.collect(Tuple2.of(world, 1));
                        }
                    }
                }
        );

        // TODO 4、分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = worldOfOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // TODO 5、聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();

        // TODO 6、执行流式数据处理
        env.execute();
    }
}
