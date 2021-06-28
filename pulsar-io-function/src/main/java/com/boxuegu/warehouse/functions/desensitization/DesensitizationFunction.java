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
import static com.boxuegu.warehouse.functions.desensitization.MatchRules.desensitization;

@Slf4j
public class DesensitizationFunction implements Function<byte[], Void> {

    final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Void process(byte[] input, Context context) {
        try {
            DesensitizationConfig desensitizationConfig = DesensitizationConfig.load(context.getUserConfigMap());
            if (desensitizationConfig.getDesensitizationTopicName() == null || desensitizationConfig.getDesensitizationCustomize() == null) {
                throw new IllegalArgumentException(" Required parameters are not set... Please check the startup script !!! ");
            }

            Map<String, String> properties = context.getCurrentRecord().getProperties();
            boolean isSend = false;
            String[] desensitizationCustomize = desensitizationConfig.getDesensitizationCustomize().split(",");
            // table A,table B
            for (String customizeStr : desensitizationCustomize) {
                String[] single = customizeStr.split(":");
                Boolean isMatch = matchTable(properties, single[0]);
                if (isMatch) {
                    // Perform Desensitization
                    JsonNode jsonNode = convert2JsonNode(input);
                    desensitization(jsonNode, single[1], single[2]);
                    context.newOutputMessage(desensitizationConfig.getDesensitizationTopicName(), Schema.BYTES)
                            .value(objectMapper.writeValueAsString(jsonNode).getBytes(StandardCharsets.UTF_8))
                            .properties(properties)
                            .send();
                    isSend = true;
                    log.info("[DesensitizationFunction perform Desensitization , send to topic {}...]",desensitizationConfig.getDesensitizationTopicName());
                    break;
                }
            }

            if (!isSend){
                context.newOutputMessage(desensitizationConfig.getDesensitizationTopicName(), Schema.BYTES)
                        .value(input)
                        .properties(properties)
                        .send();
                log.info("[DesensitizationFunction send to topic {}...]",desensitizationConfig.getDesensitizationTopicName());
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