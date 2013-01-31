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
package org.apache.hedwig.jms.spi;

import org.apache.hedwig.jms.SessionImpl;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;

/**
 *
 */
public abstract class MessageProducerImpl implements MessageProducer {

    static final int DEFAULT_PRIORITY = 4;

    private final SessionImpl session;

    // We dont really use this - since we always populate message-id : found in response of publish.
    private boolean disableMessageID = false;
    // We can support this, but dont - will overly complicate some aspects of the code : deferring for now
    // (we will need to pass this around along all failure paths).
    private boolean disableMessageTimestamp = false;
    // Hedwig supports only PERSISTENT mode, so setting to anytihng else will just cause it to be ignored.
    private int deliveryMode = DeliveryMode.PERSISTENT;
    // Hedwig does not support priorities, so everything is at default priority !
    // this does not influence actual message delivery.
    private int defaultPriority = DEFAULT_PRIORITY;
    // Hedwig does not support TTL (iirc), so we allow setting/querying this, but it has no
    // actual impact on the message delivery/expiry.
    private long timeToLive = 0;

    protected MessageProducerImpl(SessionImpl session) {
        this.session = session;
    }

    @Override
    public void setDisableMessageID(boolean disableMessageID) throws JMSException {
        this.disableMessageID = disableMessageID;
    }

    @Override
    public boolean getDisableMessageID() throws JMSException {
        return disableMessageID;
    }

    protected SessionImpl getSession() {
        return session;
    }


    @Override
    public void setDisableMessageTimestamp(boolean disableMessageTimestamp) throws JMSException {
        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        return disableMessageTimestamp;
    }

    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        if (DeliveryMode.NON_PERSISTENT != deliveryMode &&
            DeliveryMode.PERSISTENT != deliveryMode) {
            throw new JMSException("Invalid delivery mode specified : " + deliveryMode);
        }

        // if (DeliveryMode.NON_PERSISTENT == deliveryMode)
        // throw new JMSException("non-persistent delivery mode is not yet supported");
        this.deliveryMode = deliveryMode;
    }

    @Override
    public int getDeliveryMode() throws JMSException {
        return deliveryMode;
    }


    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        // Not supported, we simply allow it to be set and retrieved ...
        this.defaultPriority = defaultPriority;
    }

    @Override
    public int getPriority() throws JMSException {
        return defaultPriority;
    }


    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        this.timeToLive = timeToLive;
    }

    @Override
    public long getTimeToLive() throws JMSException {
        return timeToLive;
    }
}
