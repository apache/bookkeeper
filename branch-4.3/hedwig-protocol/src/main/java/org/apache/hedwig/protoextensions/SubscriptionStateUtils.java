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
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionStateUtils {

    static final Logger logger = LoggerFactory.getLogger(SubscriptionStateUtils.class);

    // For now, to differentiate hub subscribers from local ones, the
    // subscriberId will be prepended with a hard-coded prefix. Local
    // subscribers will validate that the subscriberId used cannot start with
    // this prefix. This is only used internally by the hub subscribers.
    public static final String HUB_SUBSCRIBER_PREFIX = "__";

    public static SubscriptionData parseSubscriptionData(byte[] data)
    throws InvalidProtocolBufferException {
        try {
            return SubscriptionData.parseFrom(data);
        } catch (InvalidProtocolBufferException ex) {
            logger.info("Failed to parse data as SubscriptionData. Fall backward to parse it as SubscriptionState for backward compatability.");
            // backward compability
            SubscriptionState state = SubscriptionState.parseFrom(data);
            return SubscriptionData.newBuilder().setState(state).build();
        }
    }

    public static String toString(SubscriptionData data) {
        StringBuilder sb = new StringBuilder();
        if (data.hasState()) {
            sb.append("State : { ").append(toString(data.getState())).append(" };");
        }
        if (data.hasPreferences()) {
            sb.append("Preferences : { ").append(toString(data.getPreferences())).append(" };");
        }
        return sb.toString();
    }

    public static String toString(SubscriptionState state) {
        StringBuilder sb = new StringBuilder();
        sb.append("consumeSeqId: " + MessageIdUtils.msgIdToReadableString(state.getMsgId()));
        return sb.toString();
    }

    public static String toString(SubscriptionPreferences preferences) {
        StringBuilder sb = new StringBuilder();
        sb.append("System Preferences : [");
        if (preferences.hasMessageBound()) {
            sb.append("(messageBound=").append(preferences.getMessageBound())
              .append(")");
        }
        sb.append("]");
        if (preferences.hasOptions()) {
            sb.append(", Customized Preferences : [");
            sb.append(MapUtils.toString(preferences.getOptions()));
            sb.append("]");
        }
        return sb.toString();
    }

    public static boolean isHubSubscriber(ByteString subscriberId) {
        return subscriberId.toStringUtf8().startsWith(HUB_SUBSCRIBER_PREFIX);
    }

    public static Map<String, ByteString> buildUserOptions(SubscriptionPreferences preferences) {
        if (preferences.hasOptions()) {
            return MapUtils.buildMap(preferences.getOptions());
        } else {
            return new HashMap<String, ByteString>();
        }
    }

}
