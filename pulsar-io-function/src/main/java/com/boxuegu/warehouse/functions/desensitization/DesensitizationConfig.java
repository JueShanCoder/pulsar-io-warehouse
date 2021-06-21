package com.boxuegu.warehouse.functions.desensitization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Data
@Accessors(chain = true)
public class DesensitizationConfig {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = " snowflake clusterID "
    )
    private String desensitizationTopicName;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = " desensitization customize eg: 'tableName:field:type,tableName1:field1:type1' "
    )
    private String desensitizationCustomize;

    public static DesensitizationConfig load(Map<String,Object> map) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(objectMapper.writeValueAsBytes(map), DesensitizationConfig.class);
    }

}