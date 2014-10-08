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
package org.apache.hedwig.client.netty;

import com.google.protobuf.ByteString;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.util.Callback;

/**
 * Handlers used by a subscription.
 */
public class FilterableMessageHandler implements MessageHandler {

    MessageHandler msgHandler;
    ClientMessageFilter  msgFilter;

    public FilterableMessageHandler(MessageHandler msgHandler,
                                    ClientMessageFilter msgFilter) {
        this.msgHandler = msgHandler;
        this.msgFilter = msgFilter;
    }

    public boolean hasMessageHandler() {
        return null != msgHandler;
    }

    public MessageHandler getMessageHandler() {
        return msgHandler;
    }

    public boolean hasMessageFilter() {
        return null != msgFilter;
    }

    public ClientMessageFilter getMessageFilter() {
        return msgFilter;
    }

    @Override
    public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                        Callback<Void> callback, Object context) {
        boolean deliver = true;
        if (hasMessageFilter()) {
            deliver = msgFilter.testMessage(msg);
        }
        if (deliver) {
            msgHandler.deliver(topic, subscriberId, msg, callback, context);
        } else {
            callback.operationFinished(context, null);
        }
    }
}
