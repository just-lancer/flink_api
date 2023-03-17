package demo;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

import java.text.SimpleDateFormat;

/**
 * @author shaco
 * @create 2023-03-14 21:24
 * @desc 自定义水位线生成策略
 */
public class Demo03 {
    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、自定义水位线生成策略
        SingleOutputStreamOperator<WebPageAccessEvent> webPageAccessEventSingleOutputStreamOperator = webPageAccessEventDS.assignTimestampsAndWatermarks(new CustomerWatermarkStrategy());
    }

    public static class CustomerWatermarkStrategy implements WatermarkStrategy<WebPageAccessEvent> {

        @Override
        public WatermarkGenerator<WebPageAccessEvent> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            WatermarkGenerator<WebPageAccessEvent> CustomerWatermarkGenerator = new WatermarkGenerator<WebPageAccessEvent>() {
                @Override
                public void onEvent(WebPageAccessEvent event, long eventTimestamp, WatermarkOutput output) {
                    // 如果需要定义非周期性生成的水位线，在这里进行定义，并使用output发送出去
                    // 那么就不需要在onPeriodicEmit方法中写水位线生成策略了
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    // 如果需要定义周期性生成的水位线，在这里进行定义，并使用output发送出去
                    // 那么就不需要在onPeriodicEmit方法中写水位线生成策略了
                }
            };

            return CustomerWatermarkGenerator;
        }
    }
}
