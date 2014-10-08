/**
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
package org.apache.hedwig.protoextensions;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.PubSubProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapUtils {

    static final Logger logger = LoggerFactory.getLogger(MapUtils.class);

    public static String toString(PubSubProtocol.Map map) {
        StringBuilder sb = new StringBuilder();
        int numEntries = map.getEntriesCount();
        for (int i=0; i<numEntries; i++) {
            PubSubProtocol.Map.Entry entry = map.getEntries(i);
            String key = entry.getKey();
            ByteString value = entry.getValue();
            sb.append(key).append('=').append(value.toStringUtf8());
            if (i != (numEntries - 1)) {
                sb.append(',');
            }
        }
        return sb.toString();
    }

    public static Map<String, ByteString> buildMap(PubSubProtocol.Map protoMap) {
        Map<String, ByteString> javaMap = new HashMap<String, ByteString>();

        int numEntries = protoMap.getEntriesCount();
        for (int i=0; i<numEntries; i++) {
            PubSubProtocol.Map.Entry entry = protoMap.getEntries(i);
            String key = entry.getKey();
            if (javaMap.containsKey(key)) {
                ByteString preValue = javaMap.get(key);
                logger.warn("Key " + key + " has already been defined as value : " + preValue.toStringUtf8());
            } else {
                javaMap.put(key, entry.getValue());
            }
        }
        return javaMap;
    }

    public static PubSubProtocol.Map.Builder buildMapBuilder(Map<String, ByteString> javaMap) {
        PubSubProtocol.Map.Builder mapBuilder = PubSubProtocol.Map.newBuilder();

        for (Map.Entry<String, ByteString> entry : javaMap.entrySet()) {
            mapBuilder.addEntries(PubSubProtocol.Map.Entry.newBuilder().setKey(entry.getKey())
                                                .setValue(entry.getValue()));
        }
        return mapBuilder;
    }
}
