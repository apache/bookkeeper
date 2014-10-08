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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.versioning.Version;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.util.Callback;

public class InMemorySubscriptionManager extends AbstractSubscriptionManager {
    // Backup for top2sub2seq
    final ConcurrentHashMap<ByteString, Map<ByteString, InMemorySubscriptionState>> top2sub2seqBackup =
        new ConcurrentHashMap<ByteString, Map<ByteString, InMemorySubscriptionState>>();

    public InMemorySubscriptionManager(ServerConfiguration conf,
                                       TopicManager tm, PersistenceManager pm,
                                       DeliveryManager dm,
                                       ScheduledExecutorService scheduler) {
        super(conf, tm, pm, dm, scheduler);
    }

    @Override
    protected void createSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData subData,
                                           Callback<Version> callback, Object ctx) {
        // nothing to do, in-memory info is already recorded by base class
        callback.operationFinished(ctx, null);
    }

    @Override
    protected void deleteSubscriptionData(ByteString topic, ByteString subscriberId, Version version, Callback<Void> callback,
                                          Object ctx) {
        // nothing to do, in-memory info is already deleted by base class
        callback.operationFinished(ctx, null);
    }

    @Override
    protected boolean isPartialUpdateSupported() {
        return false;
    }

    @Override
    protected void updateSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
                                          Version version, Callback<Version> callback, Object ctx) {
        throw new UnsupportedOperationException("Doesn't support partial update");
    }

    @Override
    protected void replaceSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
                                           Version version, Callback<Version> callback, Object ctx) {
        // nothing to do, in-memory info is already updated by base class
        callback.operationFinished(ctx, null);
    }

    @Override
    public void lostTopic(ByteString topic) {
        // Backup topic-sub2seq map for readSubscriptions
        final Map<ByteString, InMemorySubscriptionState> sub2seq = top2sub2seq.get(topic);
        if (null != sub2seq)
            top2sub2seqBackup.put(topic, sub2seq);

        if (logger.isDebugEnabled()) {
            logger.debug("InMemorySubscriptionManager is losing topic " + topic.toStringUtf8());
        }
        queuer.pushAndMaybeRun(topic, new ReleaseOp(topic, noopCallback, null));
    }

    @Override
    protected void readSubscriptions(ByteString topic,
                                     Callback<Map<ByteString, InMemorySubscriptionState>> cb, Object ctx) {
        // Since we backed up in-memory information on lostTopic, we can just return that back
        Map<ByteString, InMemorySubscriptionState> topicSubs = top2sub2seqBackup.remove(topic);

        if (topicSubs != null) {
            cb.operationFinished(ctx, topicSubs);
        } else {
            cb.operationFinished(ctx, new ConcurrentHashMap<ByteString, InMemorySubscriptionState>());
        }

    }

    @Override
    protected void readSubscriptionData(ByteString topic,
            ByteString subscriberId, Callback<InMemorySubscriptionState> cb, Object ctx) {
        // Since we backed up in-memory information on lostTopic, we can just return that back
        Map<ByteString, InMemorySubscriptionState> sub2seqBackup = top2sub2seqBackup.get(topic);
        if (sub2seqBackup == null) {
            cb.operationFinished(ctx, new InMemorySubscriptionState(
                    SubscriptionData.getDefaultInstance(), Version.NEW));
            return;
        }
        InMemorySubscriptionState subState = sub2seqBackup.remove(subscriberId);
        
        if (subState != null) {
            cb.operationFinished(ctx, subState);
        } else {
            cb.operationFinished(ctx, new InMemorySubscriptionState(
                    SubscriptionData.getDefaultInstance(), Version.NEW));
        }
    }

}
