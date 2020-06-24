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
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.api.subscription.SubscriptionStateStore;
import org.apache.distributedlog.exceptions.DLInterruptedException;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * The subscription state store Implementation.
 */
public class ZKSubscriptionStateStore implements SubscriptionStateStore {

    private static final Logger logger = LoggerFactory.getLogger(ZKSubscriptionStateStore.class);

    private final ZooKeeperClient zooKeeperClient;
    private final String zkPath;
    private static final AtomicReferenceFieldUpdater<ZKSubscriptionStateStore, DLSN> lastCommittedPositionUpdater =
        AtomicReferenceFieldUpdater.newUpdater(ZKSubscriptionStateStore.class, DLSN.class, "lastCommittedPosition");
    private volatile DLSN lastCommittedPosition = null;

    public ZKSubscriptionStateStore(ZooKeeperClient zooKeeperClient, String zkPath) {
        this.zooKeeperClient = zooKeeperClient;
        this.zkPath = zkPath;
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * Get the last committed position stored for this subscription.
     */
    @Override
    public CompletableFuture<DLSN> getLastCommitPosition() {
        DLSN dlsn = lastCommittedPositionUpdater.get(this);
        if (null != dlsn) {
            return FutureUtils.value(dlsn);
        } else {
            return getLastCommitPositionFromZK();
        }
    }

    CompletableFuture<DLSN> getLastCommitPositionFromZK() {
        final CompletableFuture<DLSN> result = new CompletableFuture<DLSN>();
        try {
            logger.debug("Reading last commit position from path {}", zkPath);
            zooKeeperClient.get().getData(zkPath, false, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    logger.debug("Read last commit position from path {}: rc = {}", zkPath, rc);
                    if (KeeperException.Code.NONODE.intValue() == rc) {
                        result.complete(DLSN.NonInclusiveLowerBound);
                    } else if (KeeperException.Code.OK.intValue() != rc) {
                        result.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
                    } else {
                        try {
                            DLSN dlsn = DLSN.deserialize(new String(data, StandardCharsets.UTF_8));
                            result.complete(dlsn);
                        } catch (Exception t) {
                            logger.warn("Invalid last commit position found from path {}", zkPath, t);
                            // invalid dlsn recorded in subscription state store
                            result.complete(DLSN.NonInclusiveLowerBound);
                        }
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException zkce) {
            result.completeExceptionally(zkce);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(new DLInterruptedException("getLastCommitPosition was interrupted", ie));
        }
        return result;
    }

    /**
     * Advances the position associated with the subscriber.
     *
     * @param newPosition - new commit position
     */
    @Override
    public CompletableFuture<Void> advanceCommitPosition(DLSN newPosition) {
        DLSN dlsn = lastCommittedPositionUpdater.get(this);
        if (null == dlsn
                || (newPosition.compareTo(dlsn) > 0)) {
            lastCommittedPositionUpdater.set(this, newPosition);
            return Utils.zkAsyncCreateFullPathOptimisticAndSetData(zooKeeperClient,
                zkPath, newPosition.serialize().getBytes(StandardCharsets.UTF_8),
                zooKeeperClient.getDefaultACL(),
                CreateMode.PERSISTENT);
        } else {
            return FutureUtils.Void();
        }
    }
}
