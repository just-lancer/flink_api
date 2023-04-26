package case_.stateprogram;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;
import utils.CustomerTimeUtils;

import java.time.Duration;

/**
 * @author shaco
 * @create 2023-04-25 10:04
 * @desc 演示示例，AggregatingState，需求：对用户点击事件，每5个数据统计一次平均时间戳
 */
public class C027_AggregatingState {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源、设置时间戳，设置水位线生成策略
        SingleOutputStreamOperator<WebPageAccessEvent> inputDS = env.addSource(new WebPageAccessEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WebPageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<WebPageAccessEvent>() {
                                            @Override
                                            public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
                                                return CustomerTimeUtils.stringToTimestamp(element.accessTime, "yyyy-MM-dd hh:mm:ss");
                                            }
                                        }
                                )
                );

        // TODO 3、将数据进行映射处理，并进行分组
        KeyedStream<Tuple2<String, Long>, String> keyedMapDS = inputDS.map(
                new MapFunction<WebPageAccessEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(WebPageAccessEvent value) throws Exception {
                        return Tuple2.of(value.userName, CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss"));
                    }
                }
        ).keyBy(
                new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                }
        );

        // TODO 4、业务逻辑的处理
        SingleOutputStreamOperator<String> outputDS = keyedMapDS.process(
                new MyProcessFunction1()
        );

        // TODO 5、将输出数据打印控制台
        inputDS.print("input");
        outputDS.print("output");

        // TODO 6、执行流数据处理
        env.execute();
    }

    private static class MyProcessFunction1 extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {
        // 定义一个状态，
        AggregatingState<Tuple2<String, Long>, Tuple2<String, Double>> userAvgTimeAggState;

        // 定义一个值状态，用于保存到来的数据数量
        ValueState<Long> countValueState;

        // 初始化状态
        @Override
        public void open(Configuration parameters) throws Exception {
            userAvgTimeAggState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Tuple2<String, Long>, Tuple3<String, Long, Long>, Tuple2<String, Double>>(
                            "acc",
                            new AggregateFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>, Tuple2<String, Double>>() {
                                @Override
                                public Tuple3<String, Long, Long> createAccumulator() {
                                    return Tuple3.of("", 0L, 0L);
                                }

                                @Override
                                public Tuple3<String, Long, Long> add(Tuple2<String, Long> value, Tuple3<String, Long, Long> accumulator) {
                                    return Tuple3.of(value.f0, accumulator.f1 + 1, accumulator.f2 + value.f1);
                                }

                                @Override
                                public Tuple2<String, Double> getResult(Tuple3<String, Long, Long> accumulator) {
                                    return Tuple2.of(accumulator.f0, (double) accumulator.f2 / (double) accumulator.f1);
                                }

                                @Override
                                public Tuple3<String, Long, Long> merge(Tuple3<String, Long, Long> a, Tuple3<String, Long, Long> b) {
                                    return null;
                                }
                            },
                            Types.TUPLE(Types.STRING, Types.LONG, Types.LONG)
                    )
            );

            countValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countDesc", Long.class));
        }

        @Override
        public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
            // 判断当前数据是不是该用户到来的第一条数据
            if (countValueState.value() == null) {
                countValueState.update(1L);
                userAvgTimeAggState.add(value);
            } else {
                countValueState.update(countValueState.value() + 1L);
                userAvgTimeAggState.add(value);
            }

            if (countValueState.value() == 5) {
                out.collect(userAvgTimeAggState.get().f0 + "：" + userAvgTimeAggState.get().f1);

                // 清空状态
                countValueState.clear();
                userAvgTimeAggState.clear();
            }
        }
    }
}
