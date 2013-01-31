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
import javax.jms.TextMessage;
import java.util.Map;

/**
 * read/write text message ...
 */
public class TextMessageImpl extends MessageImpl implements TextMessage {
    private String payload;
    private boolean readMode;

    public TextMessageImpl(SessionImpl session) {
        super(session);
        this.readMode = false;
    }

    public TextMessageImpl(SessionImpl session, String payload) {
        super(session);
        this.payload = payload;
        this.readMode = false;
    }

    public TextMessageImpl(SessionImpl session, TextMessageImpl message, String sourceTopicName,
                           String subscriberId) throws JMSException {
        super(session, (MessageImpl) message, sourceTopicName, subscriberId);

        this.payload = message.getText();
        this.readMode = message.readMode;
    }


    // To clone a message from a TextMessage which is NOT TextMessageImpl
    // Changing order of parameter to NOT accidentally clash with the constructor above.
    // This is midly confusing, but helps a lot in preventing accidental bugs !
    public TextMessageImpl(TextMessage message, SessionImpl session) throws JMSException {
        super((Message) message, session);

        if (message instanceof TextMessageImpl) {
            throw new JMSException("Coding bug - should use this constructor ONLY for non TextMessageImpl messages");
        }

        this.payload = message.getText();
        this.readMode = false;
    }

    public TextMessageImpl(SessionImpl session, PubSubProtocol.Message message, Map<String, Object> properties,
                           String sourceTopicName, String subscriberId, Runnable ackRunnable) throws JMSException {
        super(session, message, properties, sourceTopicName, subscriberId, ackRunnable);

        this.payload = hasBodyFromProperties() ? message.getBody().toStringUtf8() : null;
        this.readMode = true;
    }

    @Override
    protected MessageUtil.SupportedMessageTypes getJmsMessageType() {
        return MessageUtil.SupportedMessageTypes.TEXT;
    }

    @Override
    public PubSubProtocol.Message generateHedwigMessage() throws JMSException {
        PubSubProtocol.Message.Builder builder = PubSubProtocol.Message.newBuilder();
        super.populateBuilderWithHeaders(builder);
        if (! isBodyEmpty()) builder.setBody(ByteString.copyFromUtf8(this.payload));
        return builder.build();
    }

    protected boolean isBodyEmpty(){
        return null == this.payload;
    }

    @Override
    public void setText(String payload) throws JMSException {
        if (readMode) throw new MessageNotWriteableException("Message not writable");
        this.payload = payload;
    }

    @Override
    public String getText() throws JMSException {
        return payload;
    }

    @Override
    public void clearBody() throws JMSException {
        super.clearBody();
        this.payload = null;
        this.readMode = false;
    }

    @Override
    public void reset() throws JMSException {
        if (this.readMode) return ;
        this.readMode = true;
    }

    @Override
    TextMessageImpl createClone(SessionImpl session, String sourceTopicName, String subscriberId) throws JMSException {
        return new TextMessageImpl(session, this, sourceTopicName, subscriberId);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("TextMessageImpl");
        sb.append("{payload='").append(payload).append('\'');
        sb.append(", readMode=").append(readMode);
        sb.append(", parent=").append(super.toString());
        sb.append('}');
        return sb.toString();
    }
}
