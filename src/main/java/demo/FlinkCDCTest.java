package demo;


import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Properties;

/**
 * @author shaco
 * @create 2023-05-10 11:09
 * @desc Flink CDC测试
 */
public class FlinkCDCTest {
    public static void main(String[] args) throws Exception {

        //1.获取Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 开启CK
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/cdc-test/ck"));

        //2.通过FlinkCDC构建SourceFunction
        Properties dbProp = new Properties();
        dbProp.put("database.serverTimezone", "CTT");
        dbProp.put("snapshot.locking.mode", "none");
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("1234")
                .databaseList("test")
                .tableList("test.demo")
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new MyDeserialization()) // 自定义反序列化器
                .startupOptions(StartupOptions.initial())
                .debeziumProperties(dbProp)
                .serverId(1000)
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        //3.数据打印
        dataStreamSource.print();

        //4.启动任务
        env.execute("FlinkCDC");

    }

    // 自定义反序列化器
    public static class MyDeserialization implements DebeziumDeserializationSchema<String> {
        /**
         * {
         * "db":"",
         * "tableName":"",
         * "before":{"id":"1001","name":""...},
         * "after":{"id":"1001","name":""...},
         * "op":""
         * }
         */

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
            //创建JSON对象用于封装结果数据
            JSONObject result = new JSONObject();

            //获取库名&表名
            String topic = sourceRecord.topic();
            String[] fields = topic.split("\\.");
            result.put("db", fields[1]);
            result.put("tableName", fields[2]);

            //获取before数据
            Struct value = (Struct) sourceRecord.value();
            Struct before = value.getStruct("before");
            JSONObject beforeJson = new JSONObject();
            if (before != null) {
                //获取列信息
                Schema schema = before.schema();
                List<Field> fieldList = schema.fields();

                for (Field field : fieldList) {
                    beforeJson.put(field.name(), before.get(field));
                }
            }
            result.put("before", beforeJson);

            //获取after数据
            Struct after = value.getStruct("after");
            JSONObject afterJson = new JSONObject();
            if (after != null) {
                //获取列信息
                Schema schema = after.schema();
                List<Field> fieldList = schema.fields();

                for (Field field : fieldList) {
                    afterJson.put(field.name(), after.get(field));
                }
            }
            result.put("after", afterJson);

            //获取操作类型
            Envelope.Operation operation = Envelope.operationFor(sourceRecord);
            result.put("op", operation);

            //输出数据
            collector.collect(result.toJSONString());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

}
