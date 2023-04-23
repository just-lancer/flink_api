package case_.processfunction;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;
import utils.CustomerTimeUtils;

import java.time.Duration;

/**
 * @author shaco
 * @create 2023-04-21 13:49
 * @desc 演示示例：ProcessFunction
 */
public class C021_ProcessFunction {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源，并分配时间戳，设置水位线生成策略
        SingleOutputStreamOperator<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource())
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

        // TODO 3、直接调用process()方法
        // 定义侧输出流名称和数据类型
        OutputTag<WebPageAccessEvent> outputTag = new OutputTag<WebPageAccessEvent>("WebPagerAccessEventLate") {
        };

        SingleOutputStreamOperator<String> out = webPageAccessEventDS.process(
                new ProcessFunction<WebPageAccessEvent, String>() {
                    // 生命周期方法
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("生命周期开始");
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("生命周期结束");
                    }

                    @Override
                    public void processElement(WebPageAccessEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 获取运行时上下文对象，并调用相应的方法
                        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        System.out.println("当前并行子任务的编号是：" + numberOfParallelSubtasks);

                        // 使用上下文对象，
                        // 1、当前数据元素的时间戳
                        Long timestamp = ctx.timestamp();
                        System.out.println("当前数据元素的时间戳：" + timestamp);

                        // 2、获取定时服务，进行定时服务
                        TimerService timerService = ctx.timerService();
                        long processTime = timerService.currentProcessingTime();
                        long currentWatermark = timerService.currentWatermark();
                        // 下一行代码出错
                        timerService.registerProcessingTimeTimer(timestamp + 10 * 1000);

                        // 3、将数据输出到侧输出流
                        ctx.output(outputTag, value);

                        // 向下游输出数据
                        out.collect(value.userName);
                    }

                    // 声明定时器数据执行逻辑
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器执行逻辑
                        System.out.println("定时器到达：" + timestamp);

                    }
                }
        );

        // 获取侧输出流，并打印
        out.getSideOutput(outputTag).print(">>>>");

        // 获取输出流，并打印
        out.print("^^^^");

        // TODO 4、执行流数据处理
        env.execute();
    }
}
