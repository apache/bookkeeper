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

package org.apache.hedwig.jms.message.header;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Decouple rest of system from handling metadata/headers in hedwig.
 * Since this part might change (and is shared across the system), changes to it should be isolated from rest of
 * jms provider as much as possible so that they can evolve independently with minimal overlap.
 */
public class MetadataProcessor {

    private final static Logger logger = LoggerFactory.getLogger(MetadataProcessor.class);

    public static Map<String, Object> parseHeaders(PubSubProtocol.Message message){
        Map<String, Object> properties = new HashMap<String, Object>();

        // if not header or properties, return empty map ...
        if (! message.hasHeader() || ! message.getHeader().hasProperties()) return properties;

        // first, populate the map, then remove the standard headers from it.
        for (PubSubProtocol.Map.Entry entry : message.getHeader().getProperties().getEntriesList()){
            final JmsHeader.JmsValue value;
            try {
                ByteString data = entry.getValue();
                value = JmsHeader.JmsValue.parseFrom(data);
            } catch (InvalidProtocolBufferException e) {
                // incorrect type, we cant do much, ignore and continue.
                if (logger.isDebugEnabled()) logger.debug("Cant parse header " +
                    entry.getKey() + " as a jms value, ignoring");
                continue;
            }

            switch(value.getType()){
                case BOOLEAN:
                    properties.put(entry.getKey(), (boolean) value.getBooleanValue());
                    break;
                case BYTE:
                    properties.put(entry.getKey(), (byte) value.getByteValue());
                    break;
                case SHORT:
                    properties.put(entry.getKey(), (short) value.getShortValue());
                    break;
                case INT:
                    properties.put(entry.getKey(), (int) value.getIntValue());
                    break;
                case LONG:
                    properties.put(entry.getKey(), (long) value.getLongValue());
                    break;
                case FLOAT:
                    properties.put(entry.getKey(), (float) value.getFloatValue());
                    break;
                case DOUBLE:
                    properties.put(entry.getKey(), (double) value.getDoubleValue());
                    break;
                case STRING:
                    properties.put(entry.getKey(), (String) value.getStringValue());
                    break;
                case BYTES:
                    properties.put(entry.getKey(), value.getBytesValue());
                    break;
                default:
                    // future addition not yet supported ...
                    logger.info("Unknown metadata key type " + value.getType() +
                        " ... unsupported by jms provider. Ignoring");
                    continue;
            }
        }
        return properties;
    }

    public static void addHeaders(PubSubProtocol.Message.Builder builder, Map<String, Object> properties) {
        // Too many builder.addMetadata(...) code in this method, externalize to their own methods ? maybe later ...
        // add the user properties, and then override standard properties.

        PubSubProtocol.Map.Builder mapBuilder = PubSubProtocol.Map.newBuilder();

        for (Map.Entry<String, Object> entry : properties.entrySet()){
            // ignoring, right ?
            if (null == entry.getValue()) continue;

            final JmsHeader.JmsValue.Builder jmsValueBuilder = JmsHeader.JmsValue.newBuilder();

            final String key = entry.getKey();
            final Object value = entry.getValue();
            if (value instanceof Boolean){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.BOOLEAN);
                jmsValueBuilder.setBooleanValue((Boolean) value);
            }
            else if (value instanceof Byte){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.BYTE);
                jmsValueBuilder.setByteValue((Byte) value);
            }
            else if (value instanceof Short){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.SHORT);
                jmsValueBuilder.setShortValue((Short) value);
            }
            else if (value instanceof Integer){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.INT);
                jmsValueBuilder.setIntValue((Integer) value);
            }
            else if (value instanceof Long){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.LONG);
                jmsValueBuilder.setLongValue((Long) value);
            }
            else if (value instanceof Float){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.FLOAT);
                jmsValueBuilder.setFloatValue((Float) value);
            }
            else if (value instanceof Double){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.DOUBLE);
                jmsValueBuilder.setDoubleValue((Double) value);
            }
            else if (value instanceof String){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.STRING);
                jmsValueBuilder.setStringValue((String) value);
            }
            else if (value instanceof byte[]){
                jmsValueBuilder.setType(JmsHeader.JmsValue.ValueType.BYTES);
                jmsValueBuilder.setBytesValue(ByteString.copyFrom((byte[]) value));
            }
            else {
                throw new IllegalArgumentException("Unknown property value type ? " + entry);
            }


            PubSubProtocol.Map.Entry.Builder entryBuilder = PubSubProtocol.Map.Entry.newBuilder();
            entryBuilder.setKey(key);
            entryBuilder.setValue(jmsValueBuilder.build().toByteString());

            mapBuilder.addEntries(entryBuilder.build());
        }

        PubSubProtocol.MessageHeader.Builder messageHeaderBuilder = PubSubProtocol.MessageHeader.newBuilder();
        messageHeaderBuilder.setProperties(mapBuilder.build());

        builder.setHeader(messageHeaderBuilder.build());
    }
}
