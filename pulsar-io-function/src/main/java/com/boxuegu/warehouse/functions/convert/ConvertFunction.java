package com.boxuegu.warehouse.functions.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ConvertFunction implements Function<byte[], byte[]> {

    final ObjectMapper objectMapper = new ObjectMapper();

    private String schemaMapping;

    @Override
    public byte[] process(byte[] bytes, Context context) {
        try {
            ConvertConfig convertConfig = ConvertConfig.load(context.getUserConfigMap());
            if (convertConfig.getConvertTopicName() == null || convertConfig.getSchemaMapping() == null) {
                throw new IllegalArgumentException(" Required parameters are not set... Please check the startup script !!! ");
            }
            schemaMapping = convertConfig.getSchemaMapping();
            JsonNode jsonNode = convert2JsonNode(bytes);
            Map<String, String> properties = new HashMap<>();
            byte[] sinkBytes = convert2Byte(jsonNode, properties);
            if (sinkBytes != null) {
                log.info("send to convert topic, topic name is {}", convertConfig.getConvertTopicName());
                context.newOutputMessage(convertConfig.getConvertTopicName(), Schema.BYTES).value(sinkBytes).properties(properties).send();
            } else
                log.info("[ConvertFunction sinkBytes is null, maybe 'op' is wrong ... ]");
        } catch (Exception e) {
            log.error("[ConvertFunction got exception ..]", e);
            throw new RuntimeException(e);
        }
        return null;
    }

    private JsonNode convert2JsonNode(byte[] input) throws IOException {
        return objectMapper.readTree(new String(input));
    }

    private byte[] convert2Byte(JsonNode jsonNode, Map<String, String> properties) throws JsonProcessingException {
        String op = jsonNode.findValue("op").asText();
        String databaseName = findDataBaseName(jsonNode);
        String tableName = findTableName(jsonNode);
        properties.put("TARGET", databaseName + "." + databaseName + "." + tableName);
        properties.put("SQLMODE", "INSERT_IGNORE_INVALID");
        if (op.equalsIgnoreCase("d")) {
            // delete operator
            properties.put("ACTION", "DELETE");
            return objectMapper.writeValueAsString(jsonNode.get("before")).getBytes(StandardCharsets.UTF_8);
        } else if (op.equalsIgnoreCase("u")) {
            // update operator
            properties.put("ACTION", "UPDATE");
            return objectMapper.writeValueAsString(jsonNode.get("after")).getBytes(StandardCharsets.UTF_8);
        } else if (op.equalsIgnoreCase("c")) {
            // insert operator
            properties.put("ACTION", "INSERT");
            return objectMapper.writeValueAsString(jsonNode.get("after")).getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    private String findDataBaseName(JsonNode jsonNode) {
        String sourceDatabaseName = jsonNode.get("source").findValue("db").asText();
        return schemaMatch(schemaMapping,sourceDatabaseName);
    }

    public static String schemaMatch(String schemaMapping, String sourceDatabaseName){
        String[] schemaMatch = schemaMapping.split(",");
        for (String s: schemaMatch) {
            String[] source = s.split(":");
            if (source[0].contains(sourceDatabaseName))
                return source[1];
            else
                return sourceDatabaseName;
        }
        return sourceDatabaseName;
    }

    private String findTableName(JsonNode jsonNode) {
        return jsonNode.get("source").findValue("table").asText();
    }
}