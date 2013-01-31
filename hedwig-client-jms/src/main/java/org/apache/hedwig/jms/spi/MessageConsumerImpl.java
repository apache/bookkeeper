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

import org.apache.hedwig.jms.selector.Node;
import org.apache.hedwig.jms.selector.ParseException;
import org.apache.hedwig.jms.selector.SelectorParser;

import javax.jms.InvalidSelectorException;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;

/**
 * Base class for consumers ...
 */
public abstract class MessageConsumerImpl implements MessageConsumer {
    private final String messageSelector;
    private final Node selectorAst;
    // volatile to prevent need to lock and ensure visibility of mods across threads.
    private volatile MessageListener messageListener;

    protected MessageConsumerImpl(String msgSelector) throws InvalidSelectorException {
        {
            msgSelector = null != msgSelector ? msgSelector.trim() : null;
            this.messageSelector = (null == msgSelector || 0 == msgSelector.length()) ?
                null : msgSelector;
        }
        try {
            this.selectorAst = null == this.messageSelector ?
                null : SelectorParser.parseMessageSelector(this.messageSelector);
        } catch (ParseException pEx) {
            InvalidSelectorException jmsEx =
                new InvalidSelectorException("Unable to parse selector '" + this.messageSelector + "'");
            jmsEx.setLinkedException(pEx);
            throw jmsEx;
        }
    }

    @Override
    public String getMessageSelector() {
        return messageSelector;
    }

    public Node getSelectorAst() {
        return selectorAst;
    }

    @Override
    public MessageListener getMessageListener() {
        return messageListener;
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        this.messageListener = messageListener;
    }

}
