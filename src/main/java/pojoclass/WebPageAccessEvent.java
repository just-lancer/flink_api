package pojoclass;

/**
 * @author shaco
 * @create 2023-03-03 10:42
 * @desc 定义一个类，其实现类对象将作为流式数据源，在后续测试中一直使用
 */
public class WebPageAccessEvent {
    public String userName;
    public String url;
    public String accessTime;

    public WebPageAccessEvent(){

    }

    public WebPageAccessEvent(String userName, String url, String accessTime) {
        this.userName = userName;
        this.url = url;
        this.accessTime = accessTime;
    }

    @Override
    public String toString() {
        return "WebPageAccessEvent{" +
                "userName='" + userName + '\'' +
                ", url='" + url + '\'' +
                ", accessTime=" + accessTime +
                '}';
    }
}
