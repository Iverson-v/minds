package com.ksyun.campus.dataserver.util.jaksonutils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.*;
import com.ksyun.campus.dataserver.util.dateutil.DateUtil;


import java.io.IOException;
import java.time.LocalDateTime;

public class LocalDateTimeSerialize {

    /**
     * LocalDateTimeSerializer类是一个Jackson的自定义序列化器，用于将LocalDateTime对象序列化为字符串。
     */
    public static class LocalDateTimeSerializer extends JsonSerializer<LocalDateTime> {

        @Override
        public void serialize(LocalDateTime value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            // 将LocalDateTime对象格式化为字符串
            String timeStr = value == null ? null : DateUtil.format(value);
            // 将字符串写入JSON生成器
            gen.writeString(timeStr);
        }
    }

    /**
     * LocalDateTimeJsonDeserialize类是一个Jackson的自定义反序列化器，用于将字符串反序列化为LocalDateTime对象。
     */
    public static class LocalDateTimeJsonDeserialize extends JsonDeserializer<LocalDateTime> {

        @Override
        public LocalDateTime deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            ObjectCodec oc = p.getCodec();
            JsonNode node = oc.readTree(p);
            if (node.isNull()) {
                return null;
            }
            // 将字符串解析为LocalDateTime对象
            return DateUtil.parse(node.asText());
        }
    }
}
