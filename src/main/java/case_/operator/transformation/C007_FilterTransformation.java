package case_.operator.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;

/**
 * @author shaco
 * @create 2023-03-09 10:37
 * @desc filter算子。需求，过滤出Bob用户的访问数据
 */
public class C007_FilterTransformation {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 2、创建数据源，直接枚举数据
        DataStreamSource<WebPageAccessEvent> sampleDS = env.fromElements(
                new WebPageAccessEvent("Anna", "./start", "1000"),
                new WebPageAccessEvent("Bob", "./market", "2000"),
                new WebPageAccessEvent("Carter", "./advertising", "3000")
        );

        // TODO 3、调用filter()方法，过滤出Bob用户的访问数据
        SingleOutputStreamOperator<WebPageAccessEvent> filterDB = sampleDS.filter(
                new FilterFunction<WebPageAccessEvent>() {
                    @Override
                    public boolean filter(WebPageAccessEvent value) throws Exception {
                        return "Bob".equals(value.userName);
                    }
                }
        );

        // TODO 4、在控制台打印流数据
        filterDB.print();

        // TODO 5、执行流数据处理
        env.execute();
    }
}
