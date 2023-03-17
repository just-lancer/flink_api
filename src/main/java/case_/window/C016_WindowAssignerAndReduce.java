package case_.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @author shaco
 * @create 2023-03-17 10:57
 * @desc 窗口操作，窗口分配器 + 窗口函数，演示需求：每10秒钟计算一次用户的PV
 */
public class C016_WindowAssignerAndReduce {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、获取数据源
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、为数据分配时间戳，设置水位线生成策略
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        SingleOutputStreamOperator<WebPageAccessEvent> webPageAccessEventDSWithTime = webPageAccessEventDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WebPageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WebPageAccessEvent>() {
                                    @Override
                                    public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
                                        long parseTime = 0L;
                                        try {
                                            Date parse = sdf.parse(element.accessTime);
                                            parseTime = parse.getTime();
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        return parseTime;
                                    }
                                }
                        )
        );

        // TODO 4、对WebPageAccessEvent进行映射，并进行分组
        SingleOutputStreamOperator<Tuple2<String, Long>> reduceDS = webPageAccessEventDSWithTime.map(
                new MapFunction<WebPageAccessEvent, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(WebPageAccessEvent value) throws Exception {
                        return Tuple2.of(value.userName, 1L);
                    }
                }
        )
                .keyBy( // 按用户分组
                        new KeySelector<Tuple2<String, Long>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Long> value) throws Exception {
                                return value.f0;
                            }
                        }
                )
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(15))) // 事件时间的滑动窗口，窗口大小10，滑动步长15
                // .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(15))) // 处理时间的滚动窗口，窗口大小10，滑动步长15
                // .window(TumblingEventTimeWindows.of(Time.seconds(10)) // 事件时间的滑动窗口，窗口大小10
                // .window(TumblingProcessingTimeWindows.of(Time.seconds(10)) // 处理时间的滑动窗口，窗口大小10
                .reduce(
                        new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                // 每10秒计算一次用户PV
                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                            }
                        }
                );

        // TODO 5、输出到控制台
        webPageAccessEventDSWithTime.print();
        reduceDS.print(">>>>>>");

        // TODO 6、执行流数据处理
        env.execute();
    }
}
