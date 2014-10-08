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
package org.apache.hedwig.filter;

import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;

public interface MessageFilterBase {

    /**
     * Set subscription preferences.
     *
     * <code>preferences</code> of the subscriber will be passed to message filter when
     * the message filter attaches to its subscription either in server-side or client-side.
     *
     * @param topic
     *          Topic Name.
     * @param subscriberId
     *          Subscriber Id.
     * @param preferences
     *          Subscription Preferences.
     * @return message filter
     */
    public MessageFilterBase setSubscriptionPreferences(ByteString topic, ByteString subscriberId,
                                                        SubscriptionPreferences preferences);

    /**
     * Tests whether a particular message passes the filter or not
     *
     * @param message
     * @return
     */
    public boolean testMessage(Message message);
}
