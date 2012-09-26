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

import java.util.concurrent.CopyOnWriteArraySet;

import com.google.protobuf.ByteString;

import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.util.SubscriptionListener;

public class SubscriptionEventEmitter {

    private final CopyOnWriteArraySet<SubscriptionListener> listeners;

    public SubscriptionEventEmitter() {
        listeners = new CopyOnWriteArraySet<SubscriptionListener>();
    }

    public void addSubscriptionListener(SubscriptionListener listener) {
        listeners.add(listener); 
    }

    public void removeSubscriptionListener(SubscriptionListener listener) {
        listeners.remove(listener);
    }

    public void emitSubscriptionEvent(ByteString topic, ByteString subscriberId,
                                      SubscriptionEvent event) {
        for (SubscriptionListener listener : listeners) {
            listener.processEvent(topic, subscriberId, event);
        }
    }

}
