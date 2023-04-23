package demo;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;
import utils.CustomerTimeUtils;

import java.time.Duration;

/**
 * @author shaco
 * @create 2023-04-23 16:00
 * @desc 测试富函数的运行时上下文对象
 */
public class Demo04 {
    public static void main(String[] args) throws Exception {
        // TODO 1、获取流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // TODO 2、读取数据源，设置时间戳，设置水位线生成策略，对数据按用户名进行分组
        KeyedStream<WebPageAccessEvent, String> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource())
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

        // TODO 3、进行运行时上下文对象的测试
        SingleOutputStreamOperator<String> map = webPageAccessEventDS.map(
                new RichMapFunction<WebPageAccessEvent, String>() {
                    @Override
                    public String map(WebPageAccessEvent value) throws Exception {
                        // 获取运行时上下文对象
                        RuntimeContext runtimeContext = getRuntimeContext();

                        // 1、获取当前job的id
                        JobID jobId = runtimeContext.getJobId();
                        System.out.println("当前job的id：" + jobId);

                        String taskName = runtimeContext.getTaskName();
                        System.out.println("当前task的id：" + taskName);

                        MetricGroup metricGroup = runtimeContext.getMetricGroup();
                        System.out.println("当前并行子任务的公有组id：" + metricGroup);

                        int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();
                        System.out.println("当前task的并行子任务的数量：" + numberOfParallelSubtasks);

                        int maxNumberOfParallelSubtasks = runtimeContext.getMaxNumberOfParallelSubtasks();
                        System.out.println("当前task最大的并行子任务的数量：" + maxNumberOfParallelSubtasks);

                        int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
                        System.out.println("当前并行子task的索引：" + indexOfThisSubtask);

                        int attemptNumber = runtimeContext.getAttemptNumber();
                        System.out.println("当前并行子task的尝试次数（默认值为0）：" + attemptNumber);

                        System.out.println("============================");
                        return value.userName;
                    }
                }
        );

        // TODO 4、将输出数据打印控制台
        map.print("^^^^^^^");

        // TODO 5、执行流数据处理
        env.execute();
    }
}
