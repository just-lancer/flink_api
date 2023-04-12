package case_.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;
import utils.CustomerTimeUtils;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author shaco
 * @create 2023-04-11 15:19
 * @desc 窗口操作，全量窗口函数WindowFunction，需求：每隔20s统计一次UV
 */
public class C018_WindowAssignerAndWindowFunction {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取流数据处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、获取数据源，并分配事件的事件戳和设置水位线生成策略
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());
        SingleOutputStreamOperator<WebPageAccessEvent> eventDS = webPageAccessEventDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WebPageAccessEvent>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<WebPageAccessEvent>() {
                            @Override
                            public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
                                return CustomerTimeUtils.stringToTimestamp(element.accessTime, "yyyy-MM-dd hh:mm:ss");
                            }
                        })
        );

        // TODO 3、利用键控流统计UV：开事件时间滚动窗口，窗口大小20s
        KeyedStream<WebPageAccessEvent, String> webPageAccessEventKDS = eventDS.keyBy(
                new KeySelector<WebPageAccessEvent, String>() {
                    @Override
                    public String getKey(WebPageAccessEvent value) throws Exception {
                        return "true";
                    }
                }
        );

        SingleOutputStreamOperator<String> resultDS = webPageAccessEventKDS.window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .apply(
                        new WindowFunction<WebPageAccessEvent, String, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<WebPageAccessEvent> input, Collector<String> out) throws Exception {
                                HashSet<String> userColl = new HashSet<>();
                                Long count = 0L;
                                for (WebPageAccessEvent event : input) {
                                    if (!userColl.contains(event.userName)) {
                                        userColl.add(event.userName);
                                        count++;
                                    }
                                }
                                long end = window.getEnd();
                                long start = window.getStart();
                                out.collect(start + " ~ " + end + "，" + count);
                            }
                        }
                );

        // TODO 4、打印控制台
        eventDS.print();
        resultDS.print(">>>>");

        // TODO 5、执行流数据处理
        env.execute();
    }
}