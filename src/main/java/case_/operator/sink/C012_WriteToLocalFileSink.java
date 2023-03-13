package case_.operator.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import pojoclass.WebPageAccessEvent;
import userdefineddatasource.WebPageAccessEventSource;

import java.util.concurrent.TimeUnit;

/**
 * Author: shaco
 * Date: 2023/3/12
 * Desc: Sink算子，写入到本地文件
 */
public class C012_WriteToLocalFileSink {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 此处的并行度设置，主要是为了限制以下Sink算子，如果不设置并行度，默认使用CPU核心数量，那么会有16个分区文件
        env.setParallelism(4);

        // TODO 2、获取数据源
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.addSource(new WebPageAccessEventSource());

        // TODO 3、将数据转换成String
        SingleOutputStreamOperator<String> resDS = webPageAccessEventDS.map(
                new MapFunction<WebPageAccessEvent, String>() {
                    @Override
                    public String map(WebPageAccessEvent value) throws Exception {
                        return value.toString();
                    }
                }
        );

        // TODO 4、写入到本地文件中
        // 行编码模式
        resDS.addSink(
                // 此处的泛型方法，泛型表示需要进行持久化的数据的数据类型
                StreamingFileSink.<String>forRowFormat(new Path("./output/"), new SimpleStringEncoder<>("UTF-8"))
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder()
                                        // 文件回滚策略设置
                                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 设置非活跃时间间隔，单位：毫秒
                                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 设置文件回滚间隔，单位：毫秒
                                        .withMaxPartSize(1024 * 1024 * 1024) // 设置文件大小，单位：字节
                                        .build()
                        )
                        .build()
        );

        // TODO 流数据处理执行
        env.execute();
    }
}
