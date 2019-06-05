package com.siiconcatel.taskling.core.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import com.siiconcatel.taskling.core.TasklingExecutionException;
import com.siiconcatel.taskling.core.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class TasklingSerde {

    private static ObjectMapper mapper = new ObjectMapper()
            .registerModule(new ParameterNamesModule())
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public static <T> String serialize(T data, boolean allowNullValues) {
        if(data == null) {
            if(!allowNullValues)
                throw new TasklingExecutionException("Failed serializing an item as it was null");
        }

        try {
            return mapper.writeValueAsString(data);
        }
        catch(Exception e) {
            throw new TasklingExecutionException("Failed serializing an item. ", e);
        }
    }

    public static <T> T deserialize(Class<T> type, String json, boolean allowNullValues) {
        if(json == null) {
            if(!allowNullValues)
                throw new TasklingExecutionException("Failed deserializing an item as it was null");
        }

        if(json == null || StringUtils.isNullOrEmpty(json))
            json="{}";

        try {
            return mapper.readValue(json, type);
        }
        catch(Exception e) {
            throw new TasklingExecutionException("Failed deserializing an item. ", e);
        }
    }

    public static <T> List<String> serialize(List<T> data, boolean allowNullValues) {
        try {
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new ParameterNamesModule())
                    .registerModule(new Jdk8Module())
                    .registerModule(new JavaTimeModule());
            List<String> items = new ArrayList<>();

            for(T dataItem : data) {
                if(dataItem == null) {
                    if(!allowNullValues)
                        throw new TasklingExecutionException("Failed serializing an item as it was null");
                }

                items.add(mapper.writeValueAsString(dataItem));
            }

            return items;
        }
        catch(Exception e) {
            throw new TasklingExecutionException("Failed serializing an item. ", e);
        }
    }

    public static <T> List<T> deserialize(Class<T> type, List<String> jsonStrs, boolean allowNullValues) {
        try {
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new ParameterNamesModule())
                    .registerModule(new Jdk8Module())
                    .registerModule(new JavaTimeModule());

            List<T> items = new ArrayList<>();
            for(String json : jsonStrs) {
                if (json == null) {
                    if (!allowNullValues)
                        throw new TasklingExecutionException("Failed deserializing an item as it was null");
                }

                items.add(mapper.readValue(json, type));
            }
            return items;
        }
        catch(Exception e) {
            throw new TasklingExecutionException("Failed deserializing an item. ", e);
        }
    }
}
