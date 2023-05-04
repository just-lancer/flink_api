package case_.stateprogram;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * @author shaco
 * @create 2023-04-24 13:38
 * @desc 演示示例，Map状态，需求：实现滑动窗口，统计每个窗口中用户的访问次数
 */
public class C026_MapState {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，设置时间戳，设置水位线生成策略，对数据按用户名分组
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

        // TODO 3、业务逻辑的实现
        SingleOutputStreamOperator<ArrayList<WebPageAccessEvent>> process = inputDS.process(new MyProcessFunction(10L * 1000, 5L * 1000));

        // TODO 4执行流数据处理
        process.print("^^^^");
        inputDS.print(">>>>");
        env.execute();
    }

    static class MyProcessFunction extends KeyedProcessFunction<String, WebPageAccessEvent, ArrayList<WebPageAccessEvent>> {
        private Long windowSize; // 单位：毫秒
        private Long windowStep; // 单位：毫秒

        public MyProcessFunction(Long windowSize, Long windowStep) {
            this.windowSize = windowSize;
            this.windowStep = windowStep;
        }

        // 声明Map状态，一个key可能有多个窗口
        MapState<Long, ArrayList<WebPageAccessEvent>> webEventMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化Map状态，key表示窗口，取值为窗口的开始时间；value表示分配到该窗口的数据构成的集合
            webEventMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, ArrayList<WebPageAccessEvent>>(
                            "name",
                            TypeInformation.of(long.class),
                            TypeInformation.of(
                                    new TypeHint<ArrayList<WebPageAccessEvent>>() {
                                    }
                            )
                    )
            );
        }

        @Override
        public void processElement(WebPageAccessEvent value, Context ctx, Collector<ArrayList<WebPageAccessEvent>> out) throws Exception {
            // 当每条数据到来时，都需要确定该条数据属于哪个窗口
            // 因为是滑动窗口，所以情况有好几种
            Long windowStart;
            Long windowEnd;
            ArrayList<Tuple2<Long, Long>> windows = new ArrayList<>(); // 创建一个数组，用于存储一条数据属于哪些窗口

            // 1、size <= step时，会存在数据丢失的情况，并且数据只会存在一个窗口之中
            if (windowSize <= windowStep) {
                windowStart = CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss") / windowStep * windowStep;
                windowEnd = windowStart + windowSize;
                if (CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss") >= windowStart && CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss") < windowEnd) {
                    // 如果数据的时间戳大于等于计算出来的窗口的开始时间并且小于窗口的结束时间，那么该数据就会被窗口收纳；否则，不会，也就是窗口步长大于窗口大小时会出现的情况
                    windows.add(Tuple2.of(windowStart, windowEnd));
                }
            } else { // size > step时，同一条数据会存在多条数据中
                Long lastWindowStart = CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss") / windowStep * windowStep; // 数据所属最后一个窗口的开始时间
                for (Long everyWindowStartTime = lastWindowStart; everyWindowStartTime >= 0; everyWindowStartTime = everyWindowStartTime - windowStep) {
                    // 还得做判断，判断数据时间戳与窗口的左闭右开
                    Long everyWindowEndTime = everyWindowStartTime + windowSize;
                    if (CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss") >= everyWindowStartTime && CustomerTimeUtils.stringToTimestamp(value.accessTime, "yyyy-MM-dd hh:mm:ss") < everyWindowEndTime) {
                        windows.add(Tuple2.of(everyWindowStartTime, everyWindowEndTime));
                    }
                }
            }

            // 现在进行开窗和定时器设置
            for (Tuple2<Long, Long> element : windows) {
                // 判断当前数据是不是当前窗口的第一条数据
                if (webEventMapState.get(element.f0) == null) {
                    // 是，将当前数据添加到Map状态中，并注册定时器
                    ArrayList<WebPageAccessEvent> webPageAccessEvents = new ArrayList<>();
                    webPageAccessEvents.add(value);
                    webEventMapState.put(element.f0, webPageAccessEvents);

                    ctx.timerService().registerEventTimeTimer(element.f0 + windowSize);
                    System.out.println("定时器时间：" + (element.f0 + windowSize));
                } else {
                    // 不是，将当前数据添加到Map状态中
                    ArrayList<WebPageAccessEvent> webPageAccessEvents = webEventMapState.get(element.f0);
                    webPageAccessEvents.add(value);
                    webEventMapState.put(element.f0, webPageAccessEvents);
                }
            }
        }

        // 定时器到达
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<ArrayList<WebPageAccessEvent>> out) throws Exception {

            // 将状态中的数据发送到下游
            out.collect(webEventMapState.get(timestamp - windowSize));

//             System.out.println("清理定时器：" + (timestamp - windowSize));

            // 清理指定的窗口
            webEventMapState.remove(timestamp - windowSize);
        }
    }
}
