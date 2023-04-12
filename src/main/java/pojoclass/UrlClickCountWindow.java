package pojoclass;

import java.util.Objects;

/**
 * @author shaco
 * @create 2023-04-12 20:14
 * @desc 定义一个输出用的POJO类，用于测试用例C020的输出
 */
public class UrlClickCountWindow {
    public Long start;
    public Long end;
    public String url;
    public Long count;

    public UrlClickCountWindow() {
    }

    public UrlClickCountWindow(Long start, Long end, String url, Long count) {
        this.start = start;
        this.end = end;
        this.url = url;
        this.count = count;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UrlClickCountWindow that = (UrlClickCountWindow) o;
        return Objects.equals(start, that.start) &&
                Objects.equals(end, that.end) &&
                Objects.equals(url, that.url) &&
                Objects.equals(count, that.count);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, url, count);
    }

    @Override
    public String toString() {
        return "UrlClickCountWindow{" +
                "start=" + start +
                ", end=" + end +
                ", url='" + url + '\'' +
                ", count=" + count +
                '}';
    }
}
