package com.ksyun.campus.client.util.dateutil;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DateUtil {
    public static final String defaultPattern = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern(defaultPattern);

    /**
     * 格式化LocalDateTime对象为字符串。
     *
     * @param localDateTime 要格式化的LocalDateTime对象
     * @return 格式化后的字符串
     */
    public static String format(LocalDateTime localDateTime) {
        return DTF.format(localDateTime);
    }

    /**
     * 将字符串解析为LocalDateTime对象。
     *
     * @param value 要解析的字符串
     * @return 解析后的LocalDateTime对象
     */
    public static LocalDateTime parse(String value) {
        return LocalDateTime.parse(value, DTF);
    }
}
