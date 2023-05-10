package case_.Flink_SQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author shaco
 * @create 2023-05-09 16:25
 * @desc 创建表执行环境
 */
public class C029_CreateTableExecutionEnvironment {
    public static void main(String[] args) {
        // 1、使用TableEnvironment创建表执行环境
        // 对环境进行配置
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .inBatchMode()     // 创建批处理表执行环境
                .inStreamingMode() // 创建表处理执行环境
                .useOldPlanner()   // 使用旧版计划器，该方法已经过时
                .useAnyPlanner()   // 不显示设置计划器，默认情况使用Blink计划器
                .useBlinkPlanner() // 显示指定Blink计划器
                .build();

        TableEnvironment tableEnvironment = TableEnvironment.create(build);
        System.out.println(tableEnvironment);

        // 2、使用StreamTableEnvironment创建表执行环境
        // 先创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, build);
        System.out.println(streamTableEnvironment);
    }
}
