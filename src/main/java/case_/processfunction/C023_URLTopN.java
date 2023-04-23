package case_.processfunction;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojoclass.UrlClickCountWindow;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;
import utils.CustomerTimeUtils;

import java.time.Duration;

/**
 * @author shaco
 * @create 2023-04-21 15:37
 * @desc 综合演示案例：统计最近10s内点击数量最多的n个URL，每隔5秒更新一次
 */
public class C023_URLTopN {
    public static void main(String[] args) {
        // TODO 1、创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，设置时间戳，并设置水位线生成策略
        SingleOutputStreamOperator<WebPageAccessEvent> inputDS = env.addSource(new WebPageAccessEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WebPageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<WebPageAccessEvent>() {
                                            @Override
                                            public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
                                                return CustomerTimeUtils.stringToTimestamp(element.accessTime, "yyyy-MM-dd hh:mm:dd");
                                            }
                                        }
                                )
                );

        // TODO 3、对数据映射成url，并按url进行分组
        KeyedStream<String, String> keyedDS = inputDS.map(
                new MapFunction<WebPageAccessEvent, String>() {
                    @Override
                    public String map(WebPageAccessEvent value) throws Exception {
                        return value.url;
                    }
                }
        ).keyBy(
                new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        return value;
                    }
                }
        );

        // TODO 4、进行数据统计，使用KeyedProcessFunction
        // 在这里没有将所有的url都放入一个数据流中进行统计，这样能避免单一任务量过重
        // 第一步、开滑动窗口统计每个url的数量
        SingleOutputStreamOperator<UrlClickCountWindow> urlCount = keyedDS.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<String, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(String value, Long accumulator) {

                                return accumulator + 1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        }
                        ,
                        new ProcessWindowFunction<Long, UrlClickCountWindow, String, TimeWindow>() {
                            @Override
                            public void process(String key, Context context, Iterable<Long> elements, Collector<UrlClickCountWindow> out) throws Exception {
                                Long next = elements.iterator().next();
                                long end = context.window().getEnd();
                                long start = context.window().getStart();
                                out.collect(new UrlClickCountWindow(start, end, key, next));
                            }
                        }
                );

        // 第二步，对url的访问量进行收集，按窗口的结束时间进行分组，并进行排序
        urlCount.keyBy(
                new KeySelector<UrlClickCountWindow, Long>() {
                    @Override
                    public Long getKey(UrlClickCountWindow value) throws Exception {
                        return value.end;
                    }
                }
        ).process(
                new KeyedProcessFunction<Long, UrlClickCountWindow, Tuple2<String, Long>>() {

                    @Override
                    public void processElement(UrlClickCountWindow value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {

                    }
                }
        );

    }
}
