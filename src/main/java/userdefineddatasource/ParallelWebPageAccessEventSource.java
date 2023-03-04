package userdefineddatasource;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import pojoclass.WebPageAccessEvent;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

/**
 * @author shaco
 * @create 2023-03-03 17:18
 * @desc 自定义网页访问事件并行数据源
 */
public class ParallelWebPageAccessEventSource extends RichParallelSourceFunction<WebPageAccessEvent> {
    // 自定义数据源，每隔一秒发送一次数据，总共发送100条数据
    public Boolean isRunning = true;
    public Integer count = 0;

    public final String[] users = {"Anna", "Bob", "Carter", "David", "Eric", "Frank", "Green", "Helen", "Jerry", "Kitty"};
    public final String[] urls = {"./start", "./market", "./advertising", "./introduction", "./home", "./login", "./register", "./customer", "./searcher", "./set", "./detail", "./feedback"};
    public String user;
    public String url;

    @Override
    public void run(SourceContext ctx) throws Exception {
        Random random = new Random();
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");

        while (isRunning && count <= 100) {
            user = users[random.nextInt(users.length - 1)];
            url = urls[random.nextInt(urls.length)];
            LocalDateTime now = LocalDateTime.now();
            String dateTime = dateTimeFormatter.format(now);

            ctx.collect(new WebPageAccessEvent(user, url, dateTime));
            count++;

            Thread.sleep(1000);

            ParallelWebPageAccessEventSource stopObject = new ParallelWebPageAccessEventSource();
            stopObject.cancel();
        }


    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
