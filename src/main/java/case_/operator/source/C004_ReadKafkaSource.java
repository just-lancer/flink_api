package case_.operator.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author shaco
 * @create 2023-03-06 11:51
 * @desc 读取Kafka数据源
 */
public class C004_ReadKafkaSource {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、配置Kafka消费者属性，以及创建FlinkKafkaConsumer对象
        // Kafka消费主题
        String topic = "first";

        // Kafka连接属性
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "hadoop132:9092,hadoop133:9092"); // 集群连接地址
        kafkaProperties.put("group.id", "test"); // 设置消费者组
        kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //  key的反序列化
        kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  // value的反序列化
        kafkaProperties.put("auto.offset.reset", "latest"); // 消费偏移量，最新处开始

        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema()
                , kafkaProperties
        );

        // TODO 3、读取Kafka数据源
        DataStreamSource<String> stringKafkaDS = env.addSource(stringFlinkKafkaConsumer);

        // TODO 4、打印数据流到控制台
        stringKafkaDS.print();

        // TODO 5、执行流数据处理
        env.execute();
    }
}
