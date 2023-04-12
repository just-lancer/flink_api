package case_.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;
import utils.CustomerTimeUtils;

import java.time.Duration;
import java.util.HashSet;

/**
 * @author shaco
 * @create 2023-03-17 14:12
 * @desc 窗口操作，窗口分配器 + 增量窗口函数，演示需求：使用aggregate()，求人均重复访问量，即PV/UV
 * PV是所有站点的访问量，UV是是独立用户数量
 */
public class C017_WindowAssignerAndAggregate {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，并分配时间戳和设置水位线生成策略
        SingleOutputStreamOperator<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WebPageAccessEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<WebPageAccessEvent>() {
                                            @Override
                                            public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
                                                return CustomerTimeUtils.stringToTimestamp(element.accessTime, "yyyy-MM-dd hh:mm:ss");
                                            }
                                        }
                                )
                );

        // TODO 3、由于需要计算全部站点的访问量，因此，需要将所有的用户访问都放在一个数据流中，又因为Flink推荐使用键控流，因此，此处将所有数据直接分到同一个组里面
        SingleOutputStreamOperator<Double> aggregateDS = webPageAccessEventDS
                .keyBy(
                        new KeySelector<WebPageAccessEvent, Boolean>() {
                            @Override
                            public Boolean getKey(WebPageAccessEvent value) throws Exception {
                                return true;
                            }
                        }
                )
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(
                        new AggregateFunction<WebPageAccessEvent, Tuple2<HashSet<String>, Long>, Double>() {
                            @Override
                            public Tuple2<HashSet<String>, Long> createAccumulator() {
                                // 初始化累加器
                                return Tuple2.of(new HashSet<String>(), 0L);
                            }

                            @Override
                            public Tuple2<HashSet<String>, Long> add(WebPageAccessEvent value, Tuple2<HashSet<String>, Long> accumulator) {
                                // 进行计算
                                accumulator.f0.add(value.userName);
                                return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
                            }

                            @Override
                            public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
                                return accumulator.f1 / (double) accumulator.f0.size();
                            }

                            @Override
                            public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
                                // 合并累加器，有一些情况下，需要对多条流的累加器进行合并，需要定义合并规则
                                // 比如说，在会话窗口中，就需要进行窗口合并
                                a.f0.addAll(b.f0);
                                return Tuple2.of(a.f0, a.f1 + b.f1);
                            }
                        }
                );

        // TODO 4、打印结果到控制台
        webPageAccessEventDS.print();
        aggregateDS.print(">>>>>>");

        // TODO 5、执行流数据处理
        env.execute();
    }
}
