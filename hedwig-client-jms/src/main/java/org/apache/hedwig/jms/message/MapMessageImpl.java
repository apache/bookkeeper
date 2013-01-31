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
package org.apache.hedwig.jms.message;

import com.google.protobuf.ByteString;
import org.apache.hedwig.jms.SessionImpl;
import org.apache.hedwig.protocol.PubSubProtocol;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import java.io.IOException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * There is a weaker expectation of ordering and strong expectation of &lt;key, value&gt; container for data.
 */
public class MapMessageImpl extends MessageImpl implements MapMessage {
    private final Map<String, Object> payload = new LinkedHashMap<String, Object>(4);
    private boolean readMode;

    public MapMessageImpl(SessionImpl session) throws JMSException {
        super(session);
        clearBody();
    }

    public MapMessageImpl(SessionImpl session, MapMessageImpl message, String sourceTopicName,
                          String subscriberId) throws JMSException {
        super(session, (MessageImpl) message, sourceTopicName, subscriberId);
        this.payload.putAll(message.payload);
        this.readMode = message.readMode;
    }

    // To clone a message from a MapMessage which is NOT MapMessageImpl
    // Changing order of parameter to NOT accidentally clash with the constructor above.
    // This is midly confusing, but helps a lot in preventing accidental bugs !
    public MapMessageImpl(MapMessage message, SessionImpl session) throws JMSException {
        super((Message) message, session);

        if (message instanceof MapMessageImpl) {
            throw new JMSException("Coding bug - should use this constructor ONLY for non MapMessageImpl messages");
        }


        Enumeration keys = message.getMapNames();
        while (keys.hasMoreElements()){
            Object key = keys.nextElement();
            if (!(key instanceof String))
              throw new JMSException("Unsupported type (expected String) for key : " + key);

            String skey = (String) key;
            this.payload.put(skey, message.getObject(skey));
        }
        this.readMode = false;
    }

    @SuppressWarnings("unchecked")
    public MapMessageImpl(SessionImpl session, PubSubProtocol.Message message,
                          Map<String, Object> properties, String sourceTopicName, String subscriberId,
                          Runnable ackRunnable) throws JMSException {
        super(session, message, properties, sourceTopicName, subscriberId, ackRunnable);
        try {
            this.payload.putAll((Map<String, Object>) MessageUtil.bytesToObject(message.getBody().toByteArray()));
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to read message data .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }
        this.readMode = true;
    }

    @Override
    protected MessageUtil.SupportedMessageTypes  getJmsMessageType() {
        return MessageUtil.SupportedMessageTypes.MAP;
    }

    protected boolean isBodyEmpty(){
        return false;
    }

    @Override
    public PubSubProtocol.Message generateHedwigMessage() throws JMSException {
        PubSubProtocol.Message.Builder builder = PubSubProtocol.Message.newBuilder();
        super.populateBuilderWithHeaders(builder);

        // Now set body and type.
        try {
            builder.setBody(ByteString.copyFrom(MessageUtil.objectToBytes(this.payload)));
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to read message data .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }

        return builder.build();
    }

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return MessageUtil.asBoolean(payload.get(name));
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return MessageUtil.asByte(payload.get(name));
    }

    @Override
    public short getShort(String name) throws JMSException {
        return MessageUtil.asShort(payload.get(name));
    }

    @Override
    public char getChar(String name) throws JMSException {
        return MessageUtil.asChar(payload.get(name));
    }

    @Override
    public int getInt(String name) throws JMSException {
        return MessageUtil.asInteger(payload.get(name));
    }

    @Override
    public long getLong(String name) throws JMSException {
        return MessageUtil.asLong(payload.get(name));
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return MessageUtil.asFloat(payload.get(name));
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return MessageUtil.asDouble(payload.get(name));
    }

    @Override
    public String getString(String name) throws JMSException {
        return MessageUtil.asString(payload.get(name));
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return MessageUtil.asBytes(payload.get(name));
    }

    @Override
    public Object getObject(String name) throws JMSException {
        return payload.get(name);
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        return Collections.enumeration(payload.keySet());
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setShort(String name, short value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setChar(String name, char value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setInt(String name, int value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setLong(String name, long value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setFloat(String name, float value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setDouble(String name, double value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setString(String name, String value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setBytes(String name, byte[] value, int i, int i1) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public void setObject(String name, Object value) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        if (!MessageUtil.isValidKey(name)) throw new IllegalArgumentException("Invalid key " + name);
        payload.put(name, value);

    }

    @Override
    public boolean itemExists(String name) throws JMSException {
        return payload.containsKey(name);
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        // allow read and write.
        this.payload.clear();
        this.readMode = false;
    }

    @Override
    public void reset() throws JMSException {
        if (this.readMode) return ;
        this.readMode = true;
    }

    @Override
    MapMessageImpl createClone(SessionImpl session, String sourceTopicName, String subscriberId) throws JMSException {
        return new MapMessageImpl(session, this, sourceTopicName, subscriberId);
    }
}
