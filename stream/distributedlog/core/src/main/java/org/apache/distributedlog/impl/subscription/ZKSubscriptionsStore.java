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
package org.apache.distributedlog.impl.subscription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.api.subscription.SubscriptionStateStore;
import org.apache.distributedlog.api.subscription.SubscriptionsStore;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper Based Subscriptions Store.
 */
public class ZKSubscriptionsStore implements SubscriptionsStore {

    private final ZooKeeperClient zkc;
    private final String zkPath;
    private final ConcurrentMap<String, ZKSubscriptionStateStore> subscribers =
            new ConcurrentHashMap<String, ZKSubscriptionStateStore>();

    public ZKSubscriptionsStore(ZooKeeperClient zkc, String zkPath) {
        this.zkc = zkc;
        this.zkPath = zkPath;
    }

    private ZKSubscriptionStateStore getSubscriber(String subscriberId) {
        ZKSubscriptionStateStore ss = subscribers.get(subscriberId);
        if (ss == null) {
            ZKSubscriptionStateStore newSS = new ZKSubscriptionStateStore(zkc,
                getSubscriberZKPath(subscriberId));
            ZKSubscriptionStateStore oldSS = subscribers.putIfAbsent(subscriberId, newSS);
            if (oldSS == null) {
                ss = newSS;
            } else {
                try {
                    newSS.close();
                } catch (IOException e) {
                    // ignore the exception
                }
                ss = oldSS;
            }
        }
        return ss;
    }

    private String getSubscriberZKPath(String subscriberId) {
        return String.format("%s/%s", zkPath, subscriberId);
    }

    @Override
    public CompletableFuture<DLSN> getLastCommitPosition(String subscriberId) {
        return getSubscriber(subscriberId).getLastCommitPosition();
    }

    @Override
    public CompletableFuture<Map<String, DLSN>> getLastCommitPositions() {
        final CompletableFuture<Map<String, DLSN>> result = new CompletableFuture<Map<String, DLSN>>();
        try {
            this.zkc.get().getChildren(this.zkPath, false, new AsyncCallback.Children2Callback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                    if (KeeperException.Code.NONODE.intValue() == rc) {
                        result.complete(new HashMap<String, DLSN>());
                    } else if (KeeperException.Code.OK.intValue() != rc) {
                        result.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
                    } else {
                        getLastCommitPositions(result, children);
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            result.completeExceptionally(zkce);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(new DLInterruptedException("getLastCommitPositions was interrupted", ie));
        }
        return result;
    }

    private void getLastCommitPositions(final CompletableFuture<Map<String, DLSN>> result,
                                        List<String> subscribers) {
        List<CompletableFuture<Pair<String, DLSN>>> futures =
                new ArrayList<CompletableFuture<Pair<String, DLSN>>>(subscribers.size());
        for (String s : subscribers) {
            final String subscriber = s;
            CompletableFuture<Pair<String, DLSN>> future =
                // Get the last commit position from zookeeper
                getSubscriber(subscriber).getLastCommitPositionFromZK().thenApply(
                    dlsn -> Pair.of(subscriber, dlsn));
            futures.add(future);
        }
        FutureUtils.collect(futures).thenAccept((subscriptions) -> {
            Map<String, DLSN> subscriptionMap = new HashMap<String, DLSN>();
            for (Pair<String, DLSN> pair : subscriptions) {
                subscriptionMap.put(pair.getLeft(), pair.getRight());
            }
            result.complete(subscriptionMap);
        });
    }

    @Override
    public CompletableFuture<Void> advanceCommitPosition(String subscriberId, DLSN newPosition) {
        return getSubscriber(subscriberId).advanceCommitPosition(newPosition);
    }

    @Override
    public CompletableFuture<Boolean> deleteSubscriber(String subscriberId) {
        subscribers.remove(subscriberId);
        String path = getSubscriberZKPath(subscriberId);
        return Utils.zkDeleteIfNotExist(zkc, path, new LongVersion(-1L));
    }

    @Override
    public void close() throws IOException {
        // no-op
        for (SubscriptionStateStore store : subscribers.values()) {
            store.close();
        }
    }

}
