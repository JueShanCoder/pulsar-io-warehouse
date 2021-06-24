package com.boxuegu.warehouse.functions.convert;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.IOException;
import java.util.Map;

@Data
@Accessors(chain = true)
public class ConvertConfig {

    private static final long serialVersionUID = 1L;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = " snowflake clusterID "
    )
    private String convertTopicName;

    @FieldDoc(
            required = true,
            defaultValue = "",
            sensitive = true,
            help = " schema mapping eg: 'tableNameA:tableNameB,tableNameC:tableNameD' "
    )
    private String schemaMapping;

    public static ConvertConfig load(Map<String,Object> map) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(objectMapper.writeValueAsBytes(map), ConvertConfig.class);
    }

}