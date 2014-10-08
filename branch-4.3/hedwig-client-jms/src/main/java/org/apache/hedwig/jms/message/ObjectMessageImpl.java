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
import javax.jms.Message;
import javax.jms.MessageNotWriteableException;
import javax.jms.ObjectMessage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * read/write serializable java object ...
 *
 */
public class ObjectMessageImpl extends MessageImpl implements ObjectMessage {
    private Serializable payload;
    private boolean readMode;

    public ObjectMessageImpl(SessionImpl session, Serializable payload) {
        super(session);
        this.payload = payload;
        this.readMode = false;
    }

    public ObjectMessageImpl(SessionImpl session, ObjectMessageImpl message, String sourceTopicName,
                             String subscriberId) throws JMSException {
        super(session, (MessageImpl) message, sourceTopicName, subscriberId);

        this.payload = copySerializable(message.getObject());
        this.readMode = message.readMode;
    }

    private Serializable copySerializable(Serializable object) throws JMSException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(128);
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            oos.flush();
            oos.close();
            baos.flush();
            baos.close();

            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            return (Serializable) ois.readObject();
        } catch (IOException e){
            JMSException jmsEx = new javax.jms.IllegalStateException("Unexpected exception");
            jmsEx.setLinkedException(e);
            throw jmsEx;
        } catch (ClassNotFoundException e) {
            JMSException jmsEx = new javax.jms.IllegalStateException("Unexpected exception");
            jmsEx.setLinkedException(e);
            throw jmsEx;
        }
    }

    // To clone a message from a ObjectMessage which is NOT ObjectMessageImpl
    // Changing order of parameter to NOT accidentally clash with the constructor above.
    // This is midly confusing, but helps a lot in preventing accidental bugs !
    public ObjectMessageImpl(ObjectMessage message, SessionImpl session) throws JMSException {
        super((Message) message, session);

        if (message instanceof ObjectMessageImpl) {
            throw new JMSException("Coding bug - should use this constructor ONLY for non ObjectMessageImpl messages");
        }


        this.payload = message.getObject();
        this.readMode = false;
    }

    public ObjectMessageImpl(SessionImpl session, PubSubProtocol.Message message, Map<String, Object> properties,
                             String sourceTopicName, String subscriberId, Runnable ackRunnable) throws JMSException {
        super(session, message, properties, sourceTopicName, subscriberId, ackRunnable);

        try {
            this.payload = hasBodyFromProperties() ?
                (Serializable) MessageUtil.bytesToObject(message.getBody().toByteArray()) : null;
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to read message data .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }
        this.readMode = true;
    }

    @Override
    protected MessageUtil.SupportedMessageTypes getJmsMessageType() {
        return MessageUtil.SupportedMessageTypes.OBJECT;
    }

    @Override
    public PubSubProtocol.Message generateHedwigMessage() throws JMSException {
        PubSubProtocol.Message.Builder builder = PubSubProtocol.Message.newBuilder();
        super.populateBuilderWithHeaders(builder);

        // Now set body and type.
        try {
            if (! isBodyEmpty()) builder.setBody(ByteString.copyFrom(MessageUtil.objectToBytes(this.payload)));
        } catch (IOException e) {
            JMSException ex = new JMSException("Unable to read message data .. " + e);
            ex.setLinkedException(e);
            throw ex;
        }

        return builder.build();
    }

    protected boolean isBodyEmpty(){
        return null == this.payload;
    }

    @Override
    public void setObject(Serializable payload) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        this.payload = payload;
    }

    @Override
    public Serializable getObject() throws JMSException {
        return payload;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        // allow read and write.
        this.payload = null;
        this.readMode = false;
    }

    @Override
    public void reset() throws JMSException {
        if (this.readMode) return ;
        this.readMode = true;
    }

    @Override
    ObjectMessageImpl createClone(SessionImpl session, String sourceTopicName, String subscriberId)
        throws JMSException {

        return new ObjectMessageImpl(session, this, sourceTopicName, subscriberId);
    }
}
