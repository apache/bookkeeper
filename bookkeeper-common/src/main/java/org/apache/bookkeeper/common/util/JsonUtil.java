/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Util class which helps to parse Json to and from Object.
 */
public class JsonUtil {

    private static ObjectMapper mapper = new ObjectMapper();

    public static String toJson(Object object) throws ParseJsonException {
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
        } catch (Exception e) {
            throw new ParseJsonException("Failed to serialize Object to Json string");
        }
    }

    public static <T> T fromJson(String jsonStr, Class<T> valueType) throws ParseJsonException {
        try {
            return mapper.readValue(jsonStr, valueType);
        }  catch (Exception e) {
            throw new ParseJsonException("Failed to deserialize Object from Json string");
        }
    }

    /**
     * A JSON parsing exception.
     */
    public static class ParseJsonException extends Exception {
        public ParseJsonException(String message) {
            super(message);
        }
    }
}
