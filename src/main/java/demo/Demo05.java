package demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/**
 * Author: shaco
 * Date: 2023/5/4
 * Desc: 测试处理函数CoProcessFunction能不能使用定时器服务
 *          测试结果表明，非键控流不能使用定时服务
 */
public class Demo05 {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，设置时间戳，设置水位线生成策略
        ArrayList<Tuple2<String, Long>> tuple2s1 = new ArrayList<Tuple2<String, Long>>();
        ArrayList<Tuple2<String, Long>> tuple2s2 = new ArrayList<>();
        tuple2s1.add(Tuple2.of("a",1000L));
        tuple2s1.add(Tuple2.of("b",2000L));
        tuple2s1.add(Tuple2.of("c",3000L));
        tuple2s1.add(Tuple2.of("d",4000L));

        tuple2s2.add(Tuple2.of("A",100L));
        tuple2s2.add(Tuple2.of("B",200L));
        tuple2s2.add(Tuple2.of("C",300L));
        tuple2s2.add(Tuple2.of("D",400L));

        DataStreamSource<Tuple2<String, Long>> ds1 = env.fromCollection(tuple2s1);
        DataStreamSource<Tuple2<String, Long>> ds2 = env.fromCollection(tuple2s2);

        SingleOutputStreamOperator<Tuple2<String, Long>> ds1TS = ds1.assignTimestampsAndWatermarks(
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

        // TODO 3、进行测试
        SingleOutputStreamOperator<String> process = ds1TS.connect(ds2TS)
                .process(
                        new CoProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>() {
                            @Override
                            public void processElement1(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                                // 调用上下文对象，使用定时器
                                ctx.timerService().registerEventTimeTimer(value.f1 + 5000L);
                                out.collect(value.f0);
                            }

                            @Override
                            public void processElement2(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                                // 调用上下文对象，使用定时器服务
                                ctx.timerService().registerEventTimeTimer(value.f1 + 5000L);
                                out.collect(value.f0);
                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                                // 定时时间到达
                                System.out.println("定时器时间到达：" + timestamp);
                            }
                        }
                );

        process.print(">>>>>>");
        ds1.print("ds1");
        ds2.print("ds2");

        // TODO 2、执行流数据处理
        env.execute();
    }
}
