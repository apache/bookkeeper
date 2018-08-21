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
package org.apache.distributedlog.zk;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Watcher Manager to manage watchers.
 * <h3>Metrics</h3>
 * <ul>
 * <li> `total_watches`: total number of watches that managed by this watcher manager.
 * <li> `num_child_watches`: number of paths that are watched for children changes by this watcher manager.
 * </ul>
 */
public class ZKWatcherManager implements Watcher {

    private static final Logger logger = LoggerFactory.getLogger(ZKWatcherManager.class);

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder to build Watcher Manager.
     */
    public static class Builder {

        private String pName;
        private StatsLogger pStatsLogger;
        private ZooKeeperClient pZkc;

        public Builder name(String name) {
            this.pName = name;
            return this;
        }

        public Builder zkc(ZooKeeperClient zkc) {
            this.pZkc = zkc;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this.pStatsLogger = statsLogger;
            return this;
        }

        public ZKWatcherManager build() {
            return new ZKWatcherManager(pName, pZkc, pStatsLogger);
        }
    }

    private final String name;
    private final ZooKeeperClient zkc;
    private final StatsLogger statsLogger;
    // Gauges and their labels
    private final Gauge<Number> totalWatchesGauge;
    private static final String totalWatchesGauageLabel = "total_watches";
    private final Gauge<Number> numChildWatchesGauge;
    private static final String numChildWatchesGauageLabel = "num_child_watches";

    protected final ConcurrentMap<String, Set<Watcher>> childWatches;
    protected final LongAdder allWatchesGauge;

    private ZKWatcherManager(String name,
                             ZooKeeperClient zkc,
                             StatsLogger statsLogger) {
        this.name = name;
        this.zkc = zkc;
        this.statsLogger = statsLogger;

        // watches
        this.childWatches = new ConcurrentHashMap<String, Set<Watcher>>();
        this.allWatchesGauge = new LongAdder();

        // stats
        totalWatchesGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return allWatchesGauge.sum();
            }
        };
        this.statsLogger.registerGauge(totalWatchesGauageLabel, totalWatchesGauge);

        numChildWatchesGauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return childWatches.size();
            }
        };

        this.statsLogger.registerGauge(numChildWatchesGauageLabel, numChildWatchesGauge);
    }

    public Watcher registerChildWatcher(String path, Watcher watcher) {
        Set<Watcher> watchers = childWatches.get(path);
        if (null == watchers) {
            Set<Watcher> newWatchers = new HashSet<Watcher>();
            Set<Watcher> oldWatchers = childWatches.putIfAbsent(path, newWatchers);
            watchers = (null == oldWatchers) ? newWatchers : oldWatchers;
        }
        synchronized (watchers) {
            if (childWatches.get(path) == watchers) {
                if (watchers.add(watcher)) {
                    allWatchesGauge.increment();
                }
            } else {
                logger.warn("Watcher set for path {} has been changed while registering child watcher {}.",
                        path, watcher);
            }
        }
        return this;
    }

    public void unregisterChildWatcher(String path, Watcher watcher, boolean removeFromServer) {
        Set<Watcher> watchers = childWatches.get(path);
        if (null == watchers) {
            logger.warn("No watchers found on path {} while unregistering child watcher {}.",
                    path, watcher);
            return;
        }
        synchronized (watchers) {
            if (watchers.remove(watcher)) {
                allWatchesGauge.decrement();
            } else {
                logger.warn("Remove a non-registered child watcher {} from path {}", watcher, path);
            }
            if (watchers.isEmpty()) {
                childWatches.remove(path, watchers);
            }
        }
    }

    public void unregisterGauges() {
        this.statsLogger.unregisterGauge(totalWatchesGauageLabel, totalWatchesGauge);
        this.statsLogger.unregisterGauge(numChildWatchesGauageLabel, numChildWatchesGauge);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                handleKeeperStateEvent(event);
                break;
            case NodeChildrenChanged:
                handleChildWatchEvent(event);
                break;
            default:
                break;
        }
    }

    private void handleKeeperStateEvent(WatchedEvent event) {
        Set<Watcher> savedAllWatches = new HashSet<Watcher>((int) allWatchesGauge.sum());
        for (Set<Watcher> watcherSet : childWatches.values()) {
            synchronized (watcherSet) {
                savedAllWatches.addAll(watcherSet);
            }
        }
        for (Watcher watcher : savedAllWatches) {
            watcher.process(event);
        }
    }

    private void handleChildWatchEvent(WatchedEvent event) {
        String path = event.getPath();
        if (null == path) {
            logger.warn("Received zookeeper watch event with null path : {}", event);
            return;
        }
        Set<Watcher> watchers = childWatches.get(path);
        if (null == watchers) {
            return;
        }
        Set<Watcher> watchersToFire;
        synchronized (watchers) {
            watchersToFire = new HashSet<Watcher>(watchers.size());
            watchersToFire.addAll(watchers);
        }
        for (Watcher watcher : watchersToFire) {
            watcher.process(event);
        }
    }
}
