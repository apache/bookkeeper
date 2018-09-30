/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.clients.impl.routing;

import static com.google.common.base.Preconditions.checkState;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.StampedLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.clients.impl.internal.api.HashStreamRanges;
import org.apache.bookkeeper.common.router.HashRouter;
import org.apache.bookkeeper.stream.proto.RangeProperties;

/**
 * A router that used to route the events to ranges.
 */
@ThreadSafe
@Slf4j
public class RangeRouter<K> {

    private final HashRouter<K> keyRouter;
    private final StampedLock lock;
    private HashStreamRanges ranges;

    public RangeRouter(HashRouter<K> keyRouter) {
        this.keyRouter = keyRouter;
        this.lock = new StampedLock();
    }

    /**
     * Get the range to route the given {@code key}.
     *
     * <p>If <i>key</i> is null, a range is picked randomly. Otherwise, the range is picked
     * according to the hash code of {@code key}.
     *
     * <p>This function should be called after {@link #setRanges(HashStreamRanges)}.
     *
     * @param key the key to route
     * @return the range to write.
     * @throws IllegalStateException if ranges is empty.
     */
    public long getRange(@Nullable K key) {
        return getRangeProperties(key).getRangeId();
    }

    public RangeProperties getRangeProperties(@Nullable K key) {
        long routingKey;
        if (null != key) {
            routingKey = keyRouter.getRoutingKey(key);
        } else {
            routingKey = ThreadLocalRandom.current().nextLong();
        }
        HashStreamRanges rs;
        long stamp = lock.tryOptimisticRead();
        rs = ranges;
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                rs = ranges;
            } finally {
                lock.unlockRead(stamp);
            }
        }
        checkState(null != rs, "No range is available");

        Map.Entry<Long, RangeProperties> ceilingEntry = rs.getRanges().floorEntry(routingKey);
        return ceilingEntry.getValue();
    }

    public HashStreamRanges getRanges() {
        HashStreamRanges rs;
        long stamp = lock.tryOptimisticRead();
        rs = ranges;
        if (!lock.validate(stamp)) {
            stamp = lock.readLock();
            try {
                rs = ranges;
            } finally {
                lock.unlockRead(stamp);
            }
        }
        return rs;
    }

    public HashStreamRanges setRanges(HashStreamRanges ranges) {
        long stamp = lock.writeLock();
        try {
            // we only update the routing only when see new active ranges
            if ((this.ranges == null)
                || (ranges.getMaxRangeId() > this.ranges.getMaxRangeId())) {
                HashStreamRanges oldRanges = this.ranges;
                this.ranges = ranges;
                return oldRanges;
            } else {
                return null;
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

}
