package demo;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * @author shaco
 * @create 2023-05-11 14:10
 * @desc 实时计算演示：多表关联，统计各班级学生人数
 */
public class RealTimeComputeDemo {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、开启检查点
        env.enableCheckpointing(15 * 60 * 1000);
//        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        // 设置检查点保存路径
//        checkpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop132:9820/flink/checkpoint"));
//        // 设置检查点模式：至少一次
//        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
//        // 设置检查点超时时间：保存检查点时间为10 s
//        checkpointConfig.setCheckpointTimeout(10 * 1000L);
//        // 设置检查点并发数：1
//        checkpointConfig.setMaxConcurrentCheckpoints(1);
//        // 设置检查点间隔时间：
//        checkpointConfig.setMinPauseBetweenCheckpoints(15 * 60 * 1000);

        // TODO 3、通过Flink CDC读取数据源，为数据指定时间戳，设置水位线生成策略
        Properties dbProp = new Properties();
        dbProp.put("database.serverTimezone", "CTT");
        dbProp.put("snapshot.locking.mode", "none");
        DebeziumSourceFunction<String> studentInfoSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("1234")
                .databaseList("test")
                .tableList("test.student_info")
                .debeziumProperties(dbProp)
                .deserializer(new MyDeserialization())
                .serverId(1000)
                .startupOptions(StartupOptions.initial())
                .build();

        DebeziumSourceFunction<String> classInfoSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("1234")
                .databaseList("test")
                .tableList("test.class_info")
                .debeziumProperties(dbProp)
                .deserializer(new MyDeserialization())
                .serverId(2000)
                .startupOptions(StartupOptions.initial())
                .build();

        // 读取数据源
        DataStreamSource<String> studentDS = env.addSource(studentInfoSource, "student_source");
        DataStreamSource<String> classDS = env.addSource(classInfoSource, "class_source");

        // 将数据解析成JSON对象，然后用Tuple进行存储
        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> mapStudentDS = studentDS.map(
                new MapFunction<String, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject after = jsonObject.getJSONObject("after");
                        return Tuple3.of(after.getLong("student_id"), after.getLong("class_id"), after.getLong("create_time"));
                    }
                }
        );

        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> mapClassDS = classDS.map(
                new MapFunction<String, Tuple3<Long, Long, Long>>() {
                    @Override
                    public Tuple3<Long, Long, Long> map(String value) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        JSONObject after = jsonObject.getJSONObject("after");
                        return Tuple3.of(after.getLong("class_id"), after.getLong("school_id"), after.getLong("create_time"));
                    }
                }
        );

        // 分配分配时间戳，设置水位线生成策略
        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> tsStudentDS = mapStudentDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, Long, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple3<Long, Long, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<Long, Long, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                }
                        )
        );

        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> tsClassDS = mapClassDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<Long, Long, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Tuple3<Long, Long, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<Long, Long, Long> element, long recordTimestamp) {
                                        return element.f2;
                                    }
                                }
                        )
        );

        // TODO 4、业务逻辑处理
        // 先按班级分组，然后进行多表关联
        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> processedDS = tsStudentDS.keyBy(element -> element.f1)
                .connect(tsClassDS.keyBy(element -> element.f0))
                .process(
                        // 将两张表的数据拼接成一张表，用元组表示，分别是学生id，班级id，学校id
                        new KeyedCoProcessFunction<Long, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>>() {
                            // 声明状态描述器
                            ListState<Tuple2<Long, Long>> studentStreamState;
                            ListState<Tuple2<Long, Long>> classStreamState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                // 初始化状态
                                studentStreamState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<Long, Long>>("student_state", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                                })));
                                classStreamState = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<Long, Long>>("class_state", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                                })));
                            }

                            @Override
                            public void processElement1(Tuple3<Long, Long, Long> value, Context ctx, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
                                // 每条数据到来，都需要去另一条流中寻找数据，遍历另一条流的状态，并进行数据拼接
                                // 当前流是学生流，获取班级流
                                Iterable<Tuple2<Long, Long>> classState = classStreamState.get();
                                if (classState != null) {
                                    for (Tuple2<Long, Long> ele : classState) {
                                        out.collect(Tuple3.of(value.f0, ele.f0, ele.f1));
                                    }
                                }

                                // 添加到学生状态中
                                studentStreamState.add(Tuple2.of(value.f0, value.f1));
                            }

                            @Override
                            public void processElement2(Tuple3<Long, Long, Long> value, Context ctx, Collector<Tuple3<Long, Long, Long>> out) throws Exception {
                                // 相应的，每来一条数据，都需要去学生状态中进行匹配
                                Iterable<Tuple2<Long, Long>> stuState = studentStreamState.get();
                                if (stuState != null) {
                                    for (Tuple2<Long, Long> ele : stuState) {
                                        out.collect(Tuple3.of(ele.f0, value.f0, value.f1));
                                    }
                                }

                                // 添加到班级状态中
                                classStreamState.add(Tuple2.of(value.f0, value.f1));
                            }
                        }
                );

        // TODO 进行输出，打印控制台
//        mapStudentDS.print("student");
//        mapClassDS.print("mapClass");
        classDS.print("class");
        studentDS.print("student");
        processedDS.print();

        // TODO 执行流数据处理
        env.execute();
    }

    // 自定义数据反序列化器
    public static class MyDeserialization implements DebeziumDeserializationSchema<String> {
        /**
         * {
         * "db":"",
         * "tableName":"",
         * "before":{"id":"1001",...},
         * "after":{"id":"1001",...},
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
