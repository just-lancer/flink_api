package case_.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author shaco
 * @create 2023-03-14 20:19
 * @desc 水位线生成，代码演示
 */
public class C015_GenerateWatermark {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、为数据赋予时间戳，并设置水位线生成策略
        // Flink提供的两种水位线生成策略
        // 有序数据流水位线生成
        SingleOutputStreamOperator<WebPageAccessEvent> watermarkGenerateWay1 = webPageAccessEventDS.assignTimestampsAndWatermarks(
                // 泛型方法，泛型表示数据流中的数据类型
                WatermarkStrategy.<WebPageAccessEvent>forMonotonousTimestamps()
                        .withTimestampAssigner( // 分配时间戳
                                new SerializableTimestampAssigner<WebPageAccessEvent>() {
                                    @Override
                                    public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
                                        Long timeStamp = null;
                                        try {
                                            timeStamp = sdf.parse(element.accessTime).getTime() * 1000;
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        return timeStamp;
                                    }
                                }
                        )
        );

        // 乱序数据流水位线生成
        SingleOutputStreamOperator<WebPageAccessEvent> watermarkGenerateWay2 = webPageAccessEventDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WebPageAccessEvent>forBoundedOutOfOrderness(Duration.ofMillis(300))
                        .withTimestampAssigner( // 分配时间戳
                                new SerializableTimestampAssigner<WebPageAccessEvent>() {
                                    @Override
                                    public long extractTimestamp(WebPageAccessEvent element, long recordTimestamp) {
                                        Long timeStamp = null;
                                        try {
                                            timeStamp = sdf.parse(element.accessTime).getTime() * 1000;
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }
                                        return timeStamp;
                                    }
                                }
                        )
        );

        // TODO 4、打印到控制台
        // watermarkGenerateWay1.print(">>>>>");
        // watermarkGenerateWay2.print("-----");

        // TODO 5、执行流数据处理
        // env.execute();
    }
}
