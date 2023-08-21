package com.ksyun.campus.metaserver.util.jaksonutils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.LocalDateTime;

public class JsonMapper {

    private ObjectMapper mapper;

    /**
     * 构造函数，创建一个JsonMapper实例。
     *
     * @param include JSON序列化时包含的属性规则
     */
    public JsonMapper(JsonInclude.Include include) {
        this.mapper = new ObjectMapper();
        // 设置JSON序列化时包含的属性规则
        mapper.setSerializationInclusion(include);
        // 配置在反序列化时忽略未知的属性
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // 创建一个简单模块
        SimpleModule module = new SimpleModule();
        // 注册自定义的LocalDateTime序列化器
        module.addSerializer(LocalDateTime.class, new LocalDateTimeSerialize.LocalDateTimeSerializer());
        // 注册自定义的LocalDateTime反序列化器
        module.addDeserializer(LocalDateTime.class, new LocalDateTimeSerialize.LocalDateTimeJsonDeserialize());
        // 将模块注册到ObjectMapper中
        mapper.registerModule(module);
    }

    /**
     * 创建一个JsonMapper实例，使用默认的属性规则。
     *
     * @return JsonMapper实例
     */
    public static JsonMapper buildNormalMapper() {
        return new JsonMapper(JsonInclude.Include.ALWAYS);
    }

    /**
     * 将对象转换为JSON字符串。
     *
     * @param obj 要转换的对象
     * @return 转换后的JSON字符串
     */
    public String toJsonStr(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
            // 处理转换过程中的异常
            // 这里简单地返回空字符串
        }
        return StringUtils.EMPTY;
    }

    /**
     * 将JSON字符串转换为指定类型的Java对象。
     *
     * @param jsonStr 要转换的JSON字符串
     * @param clz     要转换的目标Java对象的类型
     * @param <T>     目标Java对象的类型参数
     * @return 转换后的Java对象
     */
    public <T> T toBean(String jsonStr, Class<T> clz) {
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        try {
            return mapper.readValue(jsonStr, clz);
        } catch (Exception e) {
            // 处理转换过程中的异常
            // 这里简单地返回null
            System.out.println("出错啦！");
        }
        return null;
    }

    /**
     * 将JSON字符串转换为指定类型的Java对象。
     *
     * @param jsonStr   要转换的JSON字符串
     * @param javaType  要转换的目标Java对象的JavaType类型
     * @param <T>       目标Java对象的类型参数
     * @return 转换后的Java对象
     */
    public <T> T toBean(String jsonStr, JavaType javaType) {
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        try {
            return mapper.readValue(jsonStr, javaType);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 将JSON字符串转换为指定类型的Java对象。
     *
     * @param jsonStr         要转换的JSON字符串
     * @param tTypeReference  要转换的目标Java对象的TypeReference类型
     * @param <T>             目标Java对象的类型参数
     * @return 转换后的Java对象
     */
    public <T> T toBean(String jsonStr, TypeReference<T> tTypeReference) {
        if (StringUtils.isEmpty(jsonStr)) {
            return null;
        }
        try {
            return mapper.readValue(jsonStr, tTypeReference);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 构造参数化类型的JavaType。
     *
     * @param parametrized       参数化类型的基本类型
     * @param parameterClasses   参数化类型的参数类型
     * @return 构造的JavaType
     */
    public JavaType constructParametricType(Class<?> parametrized, Class<?>... parameterClasses) {
        return mapper.getTypeFactory().constructParametricType(parametrized, parameterClasses);
    }
}
