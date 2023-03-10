package case_.operator.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojoclass.WebPageAccessEvent;

/**
 * @author shaco
 * @create 2023-03-10 14:43
 * @desc 富函数演示，使用RichMapFunction进行演示
 */
public class C011_RichMapFunction {
    public static void main(String[] args) throws Exception {
        // TODO 1、创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // TODO 2、创建数据源，使用流式数据
        DataStreamSource<WebPageAccessEvent> webPageAccessEventDS = env.fromElements(
                new WebPageAccessEvent("Anna", "/home", "1000"),
                new WebPageAccessEvent("Bob", "/favor", "2000")
        );

        // TODO 3、富函数演示
        SingleOutputStreamOperator<String> mapDS = webPageAccessEventDS.map(
                new RichMapFunction<WebPageAccessEvent, String>() {
                    // 生命周期方法
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        System.out.println("生命周期开始");
                    }

                    @Override
                    public void close() throws Exception {
                        System.out.println("生命周期结束");
                    }

                    @Override
                    public String map(WebPageAccessEvent value) throws Exception {
                        // 获取运行时环境上下文对象
                        RuntimeContext rc = getRuntimeContext();
                        // 获取任务id
                        System.out.println(rc.getJobId());
                        // 获取子任务索引号
                        System.out.println(rc.getIndexOfThisSubtask());
                        // 获取并行度
                        rc.getNumberOfParallelSubtasks();
                        // ......

                        return "null";
                    }
                }
        );

        mapDS.print();

        env.execute();
    }
}
