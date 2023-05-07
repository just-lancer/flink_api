package case_.checkpoint;

import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author: shaco
 * Date: 2023/5/6
 * Desc: 检查点配置
 */
public class C028_CheckPointConfig {
    public static void main(String[] args) {
        // TODO 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1、开启Flink检查点功能，设置每隔1分钟进行一次检查点。参数单位为毫秒
        env.enableCheckpointing(60 * 1000L);

        // TODO 获取检查点配置对象
        CheckpointConfig envCheckpointConfig = env.getCheckpointConfig();

        // 2、设置检查点保存路径
        // 保存到JobManager堆内存中
        envCheckpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        // 保存到外部文件系统中
        envCheckpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://hadoop132:9820/flink/checkpoint"));

        // 3、设置检查点模式：精确一次和至少一次
        // 至少一次
        envCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        // 精确一次
        envCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 4、配置检查点超时时间，时间限制为5s
        envCheckpointConfig.setCheckpointTimeout(5 * 1000L);

        // 5、配置最大并发检查点数量：1
        envCheckpointConfig.setMaxConcurrentCheckpoints(1);

        // 6、设置检查点最小间隔时间：10 s
        envCheckpointConfig.setMinPauseBetweenCheckpoints(10 * 1000L);

        // 7、配置是否开启外部持久化存储
        //  配置1：作业取消时，删除外部检查点；如果是作业失败退出，依然会保留检查点
        envCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //  配置2：作业取消时，也保留外部检查点
        envCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 8、配置检查点异常时是否让整个任务失败：配置为失败
        envCheckpointConfig.setFailOnCheckpointingErrors(false);

        // 9、配置检查点可以失败的次数，默认情况下，可以失败的次数为0，即不允许检查点失败，如果失败，将导致任务失败
        envCheckpointConfig.setTolerableCheckpointFailureNumber(0);

        // 10、配置不对齐检查点，该方法的默认值为true，即开启不对齐检查点
        envCheckpointConfig.enableUnalignedCheckpoints(true);
    }
}
