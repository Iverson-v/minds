package com.ksyun.campus.dataserver.util.jaksonutils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;

import java.util.ArrayList;
import java.util.List;

public class JacksonUtil {

    private static final JsonMapper jsonMapper = JsonMapper.buildNormalMapper();

    /**
     * 将对象转换为JSON字符串。
     *
     * @param object 要转换的对象
     * @return 转换后的JSON字符串
     */
    public static String toJsonStr(Object object) {
        return jsonMapper.toJsonStr(object);
    }

    /**
     * 将JSON字符串转换为指定类型的Java对象。
     *
     * @param jsonStr 要转换的JSON字符串
     * @param tClass  要转换的目标Java对象的类型
     * @param <T>     目标Java对象的类型参数
     * @return 转换后的Java对象
     */
    public static <T> T toBean(String jsonStr, Class<T> tClass) {
        return jsonMapper.toBean(jsonStr, tClass);
    }

    /**
     * 将JSON数组字符串转换为指定类型的Java对象列表。
     *
     * @param jsonArrayStr JSON数组字符串
     * @param tClass       要转换的目标Java对象的类型
     * @param <T>          目标Java对象的类型参数
     * @return 转换后的Java对象列表
     */
    public static <T> List<T> toBeanList(String jsonArrayStr, Class<T> tClass) {
        // 构造参数化类型的JavaType，用于指定列表元素的类型
        JavaType javaType = jsonMapper.constructParametricType(ArrayList.class, tClass);
        // 调用JsonMapper的toBean方法进行转换
        return jsonMapper.toBean(jsonArrayStr, javaType);
    }

    /**
     * 将JSON字符串转换为指定类型的Java对象。
     *
     * @param jsonStr       要转换的JSON字符串
     * @param typeReference 要转换的目标Java对象的TypeReference类型
     * @param <T>           目标Java对象的类型参数
     * @return 转换后的Java对象
     */
    public static <T> T toBean(String jsonStr, TypeReference<T> typeReference) {
        return jsonMapper.toBean(jsonStr, typeReference);
    }
}
