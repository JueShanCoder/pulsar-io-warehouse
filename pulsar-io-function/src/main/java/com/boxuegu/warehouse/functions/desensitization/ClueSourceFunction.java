package com.boxuegu.warehouse.functions.desensitization;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

@Slf4j
public class ClueSourceFunction implements Function<byte[],Void> {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Void process(byte[] bytes, Context context) throws Exception {

        return null;
    }
}