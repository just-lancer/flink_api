package case_.stateprogram;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
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
 * @create 2023-04-24 10:47
 * @desc 演示示例，值状态，需求：统计每个用户的PV数据，从第一条数据到来开始，每隔10s输出一次结果
 */
public class C024_ValueState {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，设置时间戳，设置水位线生成策略，对数据按用户进行分组
        KeyedStream<WebPageAccessEvent, String> inputDS = env.addSource(new WebPageAccessEventSource())
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
                ).keyBy(
                        new KeySelector<WebPageAccessEvent, String>() {
                            @Override
                            public String getKey(WebPageAccessEvent value) throws Exception {
                                return value.userName;
                            }
                        }
                );

        // TODO 3、对数据进行逻辑处理
        SingleOutputStreamOperator<String> outDS = inputDS.process(
                new KeyedProcessFunction<String, WebPageAccessEvent, String>() {
                    // 注册一个值状态，用于存储数据到来时的时间戳，用于注册定时器
                    ValueState<Long> timerValueState;
                    // 注册一个值状态，用于存储来了多少个数据
                    ValueState<Long> countValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化状态
                        timerValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerValueStateDescriptor", Long.class));
                        countValueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("countValueStateDescriptor", Long.class));
                    }

                    @Override
                    public void processElement(WebPageAccessEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 当数据到达时，判断countValueState是否为null，为null，表明当前到达的数据是第一条数据
                        countValueState.update(countValueState.value() == null ? 1 : countValueState.value() + 1);
                        // 当数据到达时，判断timerValueState是否为null，为null，表明此时没有定时器，需要进行定时器注册
                        if (timerValueState.value() == null) {
                            ctx.timerService().registerEventTimeTimer(CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss") + 10 * 1000);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发，输出每个用户的PV数据
                        out.collect(ctx.getCurrentKey() + " / " + CustomerTimeUtils.timeStampToString(timestamp, "yyyy-MM-dd hh:mm:ss") + " / " + countValueState.value());

                        // 清理当前key的timerValueState状态，以便于下条数据到来时创建定时器
                        timerValueState.clear();
                    }
                }
        );

        // TODO 4、将输出结果打印到控制台
        inputDS.print(">>>>");
        outDS.print();

        // TODO 5、执行流数据处理
        env.execute();
    }
}
