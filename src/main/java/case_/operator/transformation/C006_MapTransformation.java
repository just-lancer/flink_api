package case_.operator.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;

/**
 * @author shaco
 * @create 2023-03-06 14:57
 * @desc map转换算子。需求：获取每个访问事件的url
 */
public class C006_MapTransformation {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、创建数据源，简单起见，直接枚举数据
        DataStreamSource<WebPageAccessEvent> sampleDS = env.fromElements(
                new WebPageAccessEvent("Anna", "./start", "1000"),
                new WebPageAccessEvent("Bob", "./market", "2000"),
                new WebPageAccessEvent("Carter", "./advertising", "3000")
        );

        // TODO 3、调用map()方法，将WebPageAccessEvent类型的访问事件数据转换成String类型的url
        // 方式一：自定义实现类
        SingleOutputStreamOperator<String> mapDS1 = sampleDS.map(new MyMapFunction());

        // 方式二：传入匿名实现类
        SingleOutputStreamOperator<String> mapDS2 = sampleDS.map(
                new MapFunction<WebPageAccessEvent, String>() {
                    @Override
                    public String map(WebPageAccessEvent value) throws Exception {
                        return value.url;
                    }
                }
        );

        // 方式三：使用lambda表达式
        SingleOutputStreamOperator<String> mapDS3 = sampleDS.map(accessEvent -> accessEvent.url);

        // TODO 4、打印输出结果到控制台
        mapDS1.print("===");
        mapDS2.print(">>>");
        mapDS3.print("^^^");

        // TODO 5、执行流数据处理
        env.execute();
    }

    static class MyMapFunction implements MapFunction<WebPageAccessEvent, String> {
        @Override
        public String map(WebPageAccessEvent value) throws Exception {
            return value.url;
        }
    }
}
