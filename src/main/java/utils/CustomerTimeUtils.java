package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author shaco
 * @create 2023-03-17 14:21
 * @desc 用户自定义时间工具类
 */
public class CustomerTimeUtils {
    // 声明格式化对象
    static SimpleDateFormat sdf;

    // 将给定的时间字符串按指定的时间字符串格式转换成时间戳
    public static long stringToTimestamp(String strTime, String format) {
        sdf = new SimpleDateFormat(format);
        long time = 0L;
        try {
            Date parse = sdf.parse(strTime);
            time = parse.getTime();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return time;
    }

    // 将给定的毫秒时间戳转换成字符串格式
    public static String timeStampToString(long timeStamp, String format) {
        sdf = new SimpleDateFormat(format);
        long ts = String.valueOf(timeStamp).length() == 10 ? timeStamp * 1000 : timeStamp;
        Date date = new Date(ts);
        return sdf.format(date);
    }
}
