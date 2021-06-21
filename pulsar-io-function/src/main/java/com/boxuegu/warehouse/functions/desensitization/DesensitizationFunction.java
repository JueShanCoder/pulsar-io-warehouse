package com.boxuegu.warehouse.functions.desensitization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.boxuegu.warehouse.functions.desensitization.MatchRules.*;

@Slf4j
public class DesensitizationFunction implements Function<byte[],Void> {

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Void process(byte[] input, Context context) {
        try {
            DesensitizationConfig desensitizationConfig = DesensitizationConfig.load(context.getUserConfigMap());
            if (desensitizationConfig.getDesensitizationTopicName() == null || desensitizationConfig.getTargetTableName() == null
                    || desensitizationConfig.getDesensitizationField() == null){
                throw new IllegalArgumentException(" Required parameters are not set... Please check the startup script !!! ");
            }

            Map<String, String> properties = context.getCurrentRecord().getProperties();
            Boolean isMatch = matchTable(properties, desensitizationConfig.getTargetTableName());
            if (isMatch) {
                // Perform Desensitization
                JsonNode jsonNode = convert2JsonNode(input);
                for (String str: desensitizationConfig.getDesensitizationField().split(",")) {
                    String[] split = str.split(":");
                    desensitization(jsonNode,split[0],split[1]);
                }
                context.newOutputMessage(desensitizationConfig.getDesensitizationTopicName(), Schema.BYTES)
                        .value(objectMapper.writeValueAsString(jsonNode).getBytes(StandardCharsets.UTF_8))
                        .properties(properties)
                        .send();
            } else {
                context.newOutputMessage(desensitizationConfig.getDesensitizationTopicName(), Schema.BYTES)
                        .value(input)
                        .properties(properties)
                        .send();
            }
            return null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode convert2JsonNode(byte[] input) throws IOException {
        return objectMapper.readTree(new String(input));
    }
}