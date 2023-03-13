package case_.operator.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

/**
 * @author shaco
 * @create 2023-03-13 14:40
 * @desc Sink算子，写出到Kafka
 */
public class C013_WriteToKafkaSink {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、读取数据源
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、转换成String直接写入到Kafka
        SingleOutputStreamOperator<String> mapDS = webPageAccessEventDS.map(
                new MapFunction<WebPageAccessEvent, String>() {
                    @Override
                    public String map(WebPageAccessEvent value) throws Exception {
                        return value.toString();
                    }
                }
        );

        mapDS.addSink(
                new FlinkKafkaProducer<String>("hadoop132:9092", "sink_topic", new SimpleStringSchema())
        );

        // TODO 4、执行流数据处理
        env.execute();
    }
}
