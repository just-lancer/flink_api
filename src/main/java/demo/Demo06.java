package demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author shaco
 * @create 2023-05-08 12:53
 * @desc 测试广播流的key-value中，key是什么，怎么设置
 * 结论是：当需要被广播的那条流，来了一条数据后，那么就需要对这条数据进行处理，设置key,value，然后写入到广播流中
 */
public class Demo06 {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，设置时间戳，设置水位线生成策略
        ArrayList<Tuple3<String, Long, String>> tuple2s1 = new ArrayList<Tuple3<String, Long, String>>();
        ArrayList<Tuple2<String, Long>> tuple2s2 = new ArrayList<>();
        tuple2s1.add(Tuple3.of("a", 1000L, "hello"));
        tuple2s1.add(Tuple3.of("b", 2000L, "hello"));
        tuple2s1.add(Tuple3.of("c", 3000L, "hello"));
        tuple2s1.add(Tuple3.of("d", 4000L, "hello"));

        tuple2s2.add(Tuple2.of("A", 100L));
        tuple2s2.add(Tuple2.of("B", 200L));
        tuple2s2.add(Tuple2.of("C", 300L));
        tuple2s2.add(Tuple2.of("D", 400L));

        DataStreamSource<Tuple3<String, Long, String>> ds1 = env.fromCollection(tuple2s1);
        DataStreamSource<Tuple2<String, Long>> ds2 = env.fromCollection(tuple2s2);

        SingleOutputStreamOperator<Tuple3<String, Long, String>> ds1TS = ds1.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Long, String>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple3<String, Long, String>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, Long, String> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<Tuple2<String, Long>> ds2TS = ds2.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                        return element.f1;
                                    }
                                }
                        )
        );

        // TODO 开始测试
        // 创建MapState状态
        MapStateDescriptor<String, Long> broadcast_test = new MapStateDescriptor<>("broadcast_test", String.class, Long.class);

        // 将第一条数据流进行广播
        BroadcastStream<Tuple3<String, Long, String>> broadcast = ds1.broadcast(broadcast_test);

        // 用第二条流去关联第一条流
        SingleOutputStreamOperator<String> process = ds2.connect(broadcast)
                .process(
                        new BroadcastProcessFunction<Tuple2<String, Long>, Tuple3<String, Long, String>, String>() {
                            @Override
                            public void processElement(Tuple2<String, Long> value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                                // 获取广播状态
                                ReadOnlyBroadcastState<String, Long> broadcastState1 = ctx.getBroadcastState(broadcast_test);

                                // 使用广播状态，直接打印
                                Iterable<Map.Entry<String, Long>> entries = broadcastState1.immutableEntries();
                                for (Map.Entry<String, Long> ele : entries) {
                                    System.out.println(ele);
                                }
                            }

                            @Override
                            public void processBroadcastElement(Tuple3<String, Long, String> value, Context ctx, Collector<String> out) throws Exception {
                                // 设置广播状态的key，全部设置为hello；设置广播状态的value，全部设置为1000
                                BroadcastState<String, Long> broadcastState2 = ctx.getBroadcastState(broadcast_test);
                                broadcastState2.put("hello", 1000L);
                            }
                        }
                );

        // 执行流数据处理
        env.execute();
    }
}
