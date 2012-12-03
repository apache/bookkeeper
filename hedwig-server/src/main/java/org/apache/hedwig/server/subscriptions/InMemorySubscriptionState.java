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
package org.apache.hedwig.server.subscriptions;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.versioning.Version;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.protoextensions.MapUtils;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;

public class InMemorySubscriptionState {
    SubscriptionState subscriptionState;
    SubscriptionPreferences subscriptionPreferences;
    MessageSeqId lastConsumeSeqId;
    Version version;
    long lastPersistedSeqId;

    public InMemorySubscriptionState(SubscriptionData subscriptionData, Version version, MessageSeqId lastConsumeSeqId) {
        this.subscriptionState = subscriptionData.getState();
        if (subscriptionData.hasPreferences()) {
            this.subscriptionPreferences = subscriptionData.getPreferences();
        } else {
            // set initial subscription preferences
            SubscriptionPreferences.Builder prefsBuilder = SubscriptionPreferences.newBuilder();
            // progate the old system preferences from subscription state to preferences
            prefsBuilder.setMessageBound(subscriptionState.getMessageBound());
            this.subscriptionPreferences = prefsBuilder.build();

        }
        this.lastConsumeSeqId = lastConsumeSeqId;
        this.version = version;
        this.lastPersistedSeqId = subscriptionState.getMsgId().getLocalComponent();
    }

    public InMemorySubscriptionState(SubscriptionData subscriptionData, Version version) {
        this(subscriptionData, version, subscriptionData.getState().getMsgId());
    }

    public SubscriptionData toSubscriptionData() {
        SubscriptionState.Builder stateBuilder =
            SubscriptionState.newBuilder(subscriptionState).setMsgId(lastConsumeSeqId);
        return SubscriptionData.newBuilder().setState(stateBuilder)
                                            .setPreferences(subscriptionPreferences)
                                            .build();
    }

    public SubscriptionState getSubscriptionState() {
        return subscriptionState;
    }

    public SubscriptionPreferences getSubscriptionPreferences() {
        return subscriptionPreferences;
    }

    public MessageSeqId getLastConsumeSeqId() {
        return lastConsumeSeqId;
    }
     
    public Version getVersion() {
        return version;
    }
    
    public void setVersion(Version version) {
        this.version = version;
    }

    /**
     *
     * @param lastConsumeSeqId
     * @param consumeInterval
     *            The amount of laziness we want in persisting the consume
     *            pointers
     * @return true if the resulting structure needs to be persisted, false
     *         otherwise
     */
    public boolean setLastConsumeSeqId(MessageSeqId lastConsumeSeqId, int consumeInterval) {
        long interval = lastConsumeSeqId.getLocalComponent() - subscriptionState.getMsgId().getLocalComponent();
        if (interval <= 0) {
            return false;
        }

        // set consume seq id when it is larger
        this.lastConsumeSeqId = lastConsumeSeqId;
        if (interval < consumeInterval) {
            return false;
        }

        // subscription state will be updated, marked it as clean
        subscriptionState = SubscriptionState.newBuilder(subscriptionState).setMsgId(lastConsumeSeqId).build();
        return true;
    }

    /**
     * Set lastConsumeSeqId Immediately
     *
     * @return true if the resulting structure needs to be persisted, false otherwise
     */
    public boolean setLastConsumeSeqIdImmediately() {
        long interval = lastConsumeSeqId.getLocalComponent() - subscriptionState.getMsgId().getLocalComponent();
        // no need to set
        if (interval <= 0) {
            return false;
        }
        subscriptionState = SubscriptionState.newBuilder(subscriptionState).setMsgId(lastConsumeSeqId).build();
        return true;
    }

    public long getLastPersistedSeqId() {
        return lastPersistedSeqId;
    }

    public void setLastPersistedSeqId(long lastPersistedSeqId) {
        this.lastPersistedSeqId = lastPersistedSeqId;
    }

    /**
     * Update preferences.
     *
     * @return true if preferences is updated, which needs to be persisted, false otherwise.
     */
    public boolean updatePreferences(SubscriptionPreferences preferences) {
        boolean changed = false;
        SubscriptionPreferences.Builder newPreferencesBuilder = SubscriptionPreferences.newBuilder(subscriptionPreferences);
        if (preferences.hasMessageBound()) {
            if (!subscriptionPreferences.hasMessageBound() ||
                subscriptionPreferences.getMessageBound() != preferences.getMessageBound()) {
                newPreferencesBuilder.setMessageBound(preferences.getMessageBound());
                changed = true;
            }
        }
        if (preferences.hasMessageFilter()) {
            if (!subscriptionPreferences.hasMessageFilter() ||
                !subscriptionPreferences.getMessageFilter().equals(preferences.getMessageFilter())) {
                newPreferencesBuilder.setMessageFilter(preferences.getMessageFilter());
                changed = true;
            }
        }
        if (preferences.hasMessageWindowSize()) {
            if (!subscriptionPreferences.hasMessageWindowSize() ||
                subscriptionPreferences.getMessageWindowSize() !=
                preferences.getMessageWindowSize()) {
                newPreferencesBuilder.setMessageWindowSize(preferences.getMessageWindowSize());
                changed = true;
            }
        }
        if (preferences.hasOptions()) {
            Map<String, ByteString> userOptions = SubscriptionStateUtils.buildUserOptions(subscriptionPreferences);
            Map<String, ByteString> optUpdates = SubscriptionStateUtils.buildUserOptions(preferences);
            boolean optChanged = false;
            for (Map.Entry<String, ByteString> entry : optUpdates.entrySet()) {
                String key = entry.getKey();
                if (userOptions.containsKey(key)) {
                    if (null == entry.getValue()) {
                        userOptions.remove(key);
                        optChanged = true;
                    } else {
                        if (!entry.getValue().equals(userOptions.get(key))) {
                            userOptions.put(key, entry.getValue());
                            optChanged = true;
                        }
                    }
                } else {
                    userOptions.put(key, entry.getValue());
                    optChanged = true;
                }
            }
            if (optChanged) {
                changed = true;
                newPreferencesBuilder.setOptions(MapUtils.buildMapBuilder(userOptions));
            }
        }
        if (changed) {
            subscriptionPreferences = newPreferencesBuilder.build();
        }
        return changed;
    }

}
