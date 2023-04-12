package case_.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * @create 2023-04-12 18:04
 * @desc 窗口操作，增量窗口函数和全量窗口函数结合使用，统计10秒内，url的点击数量，每隔5秒更新一次
 */
public class C020_WindowOperateIncreseAndFullWindowFunciton {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，设置时间戳，设置水位线生成策略
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());
        SingleOutputStreamOperator<WebPageAccessEvent> webPageAccessEventTDS = webPageAccessEventDS.assignTimestampsAndWatermarks(
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

        // TODO 3、数据处理
        // 按键分组，根据url进行分组
        KeyedStream<WebPageAccessEvent, String> webPageAccessEventKDS = webPageAccessEventTDS.keyBy(
                new KeySelector<WebPageAccessEvent, String>() {
                    @Override
                    public String getKey(WebPageAccessEvent value) throws Exception {
                        return value.url;
                    }
                }
        );

        // 开窗，开滑动窗口，窗口大小10，滑动步长5
        SingleOutputStreamOperator<UrlClickCountWindow> urlClick = webPageAccessEventKDS.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<WebPageAccessEvent, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(WebPageAccessEvent value, Long accumulator) {
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
                        // 输出数据类型为UrlClickCountWindow，各属性分别代表：窗口开始时间，窗口结束时间，url，url点击的次数
                        new ProcessWindowFunction<Long, UrlClickCountWindow, String, TimeWindow>() {
                            @Override
                            public void process(String url, Context context, Iterable<Long> elements, Collector<UrlClickCountWindow> out) throws Exception {
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                out.collect(new UrlClickCountWindow(start, end, url, elements.iterator().next()));
                            }
                        }
                );

        // TODO 4、打印输出流到控制台
        urlClick.print(">>>>");

        // TODO 5、执行流数据处理
        env.execute();
    }
}
