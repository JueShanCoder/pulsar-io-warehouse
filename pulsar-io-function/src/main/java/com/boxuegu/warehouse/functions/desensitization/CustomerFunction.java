package com.boxuegu.warehouse.functions.desensitization;


import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

@Slf4j
public class CustomerFunction implements Function<byte[],Void>{

    @Override
    public Void process(byte[] input, Context context) throws Exception {

        return null;
    }

}