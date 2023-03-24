//package case_.window;
//
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
//import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import pojoclass.WebPageAccessEvent;
//import userdefineddatasource.WebPageAccessEventSource;
//import utils.CustomerTimeUtils;
//
//import java.time.Duration;
//
///**
// * @author shaco
// * @create 2023-03-17 17:58
// * @desc 窗口操作，窗口分配器 + 全量窗口函数，演示需求：使用WindowFunction，计算每个用户每个窗口中位访问记录
// */
//public class C018_WindowAssignerAndWindowFunction {
//    public static void main(String[] args) {
//        // TODO 1、创建流执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        // TODO 2、读取数据源，并分配时间戳和设置水位线生成策略
//        SingleOutputStreamOperator<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource())
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<WebPageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                                .withTimestampAssigner(
//                                        new SerializableTimestampAssigner<WebPageAccessEvent>() {
//                                            @Override
//                                            public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
//                                                return CustomerTimeUtils.stringToTimestamp(element.accessTime, "yyyy-MM-dd hh:mm:ss");
//                                            }
//                                        }
//                                )
//                );
//
//        // TODO 3、对用户进行分组，并设置窗口
//        webPageAccessEventDS.keyBy(
//                new KeySelector<WebPageAccessEvent, String>() {
//                    @Override
//                    public String getKey(WebPageAccessEvent value) throws Exception {
//                        return value.userName;
//                    }
//                }
//        )
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .apply(
//                        new WindowFunction<WebPageAccessEvent, WebPageAccessEvent, String, TimeWindow>() {
//                        }
//                );
//
//        // 如果不分组会是什么样
//        env.addSource(new WebPageAccessEventSource()).windowAll(TumblingEventTimeWindows.of(Time.seconds(10))).apply(new AllWindowFunction<WebPageAccessEvent, Object, TimeWindow>() {
//        });
//
//    }
//}
