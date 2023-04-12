package case_.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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
 * @create 2023-04-12 10:21
 * @desc 窗口操作，全量窗口函数ProcessWindowFunction，需求：每隔20s统计一次UV
 */
public class C019_WindowAssignerAndProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取流数据处理环镜
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、获取数据源，并为数据源设置事件时间戳，并设置水位线生成策略
        DataStreamSource<WebPageAccessEvent> inputDS = env.addSource(new WebPageAccessEventSource());
        SingleOutputStreamOperator<WebPageAccessEvent> DS = inputDS.assignTimestampsAndWatermarks(
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

        // TODO 3、开窗，进行数据统计，开启20秒滚动窗口
        SingleOutputStreamOperator<String> process = DS.keyBy(
                new KeySelector<WebPageAccessEvent, String>() {
                    @Override
                    public String getKey(WebPageAccessEvent value) throws Exception {
                        return "true";
                    }
                }
        ).window(TumblingEventTimeWindows.of(Time.seconds(20)))
                .process(
                        new ProcessWindowFunction<WebPageAccessEvent, String, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<WebPageAccessEvent> elements, Collector<String> out) throws Exception {
                                HashSet<String> userColl = new HashSet<>();

                                for (WebPageAccessEvent event : elements) {
                                    userColl.add(event.userName);
                                }

                                long end = context.window().getEnd();
                                long start = context.window().getStart();
                                out.collect("窗口：" + start + " ~ " + end + " " + userColl.size() + "");

                            }
                        }
                );

        process.print(">>>>");
        inputDS.print();

        env.execute();
    }
}
