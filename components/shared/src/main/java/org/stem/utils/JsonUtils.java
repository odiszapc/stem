/*
 * Copyright 2014 Alexey Plotnik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.stem.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class JsonUtils {

    private static ObjectMapper mapper = new ObjectMapper();

    public static TypeFactory getTypeFactory() {
        return mapper.getTypeFactory();
    }

    static {
        //jsonEncoder.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        //jsonEncoder.configure(JsonGenerator.Feature.QUOTE_FIELD_NAMES, false);
    }

    public static String encode(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

    public static String encodeFormatted(Object obj) {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

    public static byte[] encodeBytes(Object obj) {
        try {
            return mapper.writeValueAsBytes(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

    public static <T> T decode(String json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

    public static <T> T decode(byte[] json, Class<T> clazz) {
        try {
            return mapper.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

    public static <T> T decode(InputStream jsonStream, Class<T> clazz) {
        try {
            return mapper.readValue(jsonStream, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

    public static <T> T decode(InputStream jsonStream, SimpleType typeInfo) {
        try {
            return mapper.readValue(jsonStream, typeInfo);
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

    public static <T> T decode(String json, JavaType type) {
        try {
            return mapper.readValue(json, type);
        } catch (IOException e) {
            throw new RuntimeException("Can't serialize object", e);
        }
    }

//    public static JavaType constructCustomType() {
//        mapper.getTypeFactory()
//                .moreSpecificType()
//    }

    public static JavaType constructHashOfLists(Class<?> keyClass, Class<?> listElementClass) {
        JavaType keyType = mapper.getTypeFactory().uncheckedSimpleType(keyClass);

        CollectionType listType = mapper.getTypeFactory().
                constructCollectionType(List.class, listElementClass);

        JavaType type = mapper.getTypeFactory().
                constructMapType(Map.class, keyType, listType);

        return type;
    }
}