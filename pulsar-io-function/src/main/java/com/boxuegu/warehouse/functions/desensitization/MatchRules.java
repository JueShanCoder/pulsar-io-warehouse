package com.boxuegu.warehouse.functions.desensitization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import red.zyc.desensitization.Sensitive;
import red.zyc.desensitization.annotation.*;
import red.zyc.desensitization.support.TypeToken;

import java.util.Map;

@Slf4j
public class MatchRules {

    // if need to add rules, please add in following type
    private static final String STRING = "STRING";
    private static final String ALL_STRING = "ALL_STRING";
    private static final String EMAIL = "EMAIL";
    private static final String PHONE = "PHONE";
    private static final String CHINESE_NAME = "CHINESE_NAME";
    private static final String BANK_ID = "BANK_ID";
    private static final String PASSWORD = "PASSWORD";

    public static Boolean matchTable(Map<String,String> properties, String tableName){
        String target = properties.get("TARGET");
        return target.contains(tableName);
    }

    public static void desensitization(JsonNode jsonNode,String field, String desensitizationType) throws Exception {
        String target = jsonNode.get(field).asText();
        String desensitize = desensitizationOperator(target,desensitizationType);

        if (desensitize == null)
            throw new IllegalArgumentException(" Desensitization type not match ...  Please check the startup script !!! ");
        ((ObjectNode) jsonNode).put(field, desensitize);
    }

    public static String desensitizationOperator(String target,String desensitizationType) {
        if (desensitizationType.equals(STRING))
            return Sensitive.desensitize(target, new TypeToken<@UsccSensitive String>() {
            });

        if (desensitizationType.equals(ALL_STRING))
            return Sensitive.desensitize(target, new TypeToken<@CharSequenceSensitive String>() {
            });

        if (desensitizationType.equals(EMAIL))
            return Sensitive.desensitize(target, new TypeToken<@EmailSensitive String>() {
            });

        if (desensitizationType.equals(PHONE))
            return Sensitive.desensitize(target, new TypeToken<@PhoneNumberSensitive String>() {
            });

        if (desensitizationType.equals(CHINESE_NAME))
            return Sensitive.desensitize(target, new TypeToken<@ChineseNameSensitive String>() {
            });

        if (desensitizationType.equals(BANK_ID))
            return Sensitive.desensitize(target, new TypeToken<@BankCardNumberSensitive String>() {
            });

        if (desensitizationType.equals(PASSWORD))
            return Sensitive.desensitize(target, new TypeToken<@PasswordSensitive String>() {
            });

        return null;
    }
}