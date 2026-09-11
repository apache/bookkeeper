/*
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
package org.apache.distributedlog.impl;

import static org.apache.distributedlog.util.DLUtils.isReservedStreamName;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.namespace.NamespaceWatcher;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

/**
 * Watcher on watching a given namespace.
 */
@CustomLog
public class ZKNamespaceWatcher extends NamespaceWatcher
        implements Runnable, Watcher, AsyncCallback.Children2Callback {

    private final DistributedLogConfiguration conf;
    private final URI uri;
    private final ZooKeeperClient zkc;
    private final OrderedScheduler scheduler;
    private final AtomicBoolean namespaceWatcherSet = new AtomicBoolean(false);

    public ZKNamespaceWatcher(DistributedLogConfiguration conf,
                              URI uri,
                              ZooKeeperClient zkc,
                              OrderedScheduler scheduler) {
        this.conf = conf;
        this.uri = uri;
        this.zkc = zkc;
        this.scheduler = scheduler;
    }

    private void scheduleTask(Runnable r, long ms) {
        try {
            scheduler.schedule(r, ms, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            log.error()
                    .attr("task", r)
                    .attr("delayMs", ms)
                    .exception(ree)
                    .log("Task scheduled is rejected");
        }
    }

    @Override
    public void run() {
        try {
            doWatchNamespaceChanges();
        } catch (Exception e) {
            log.error().attr("uri", uri).exception(e).log("Encountered unknown exception on watching namespace");
        }
    }

    public void watchNamespaceChanges() {
        if (!namespaceWatcherSet.compareAndSet(false, true)) {
            return;
        }
        doWatchNamespaceChanges();
    }

    private void doWatchNamespaceChanges() {
        try {
            zkc.get().getChildren(uri.getPath(), this, this, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn().attr("uri", uri).exception(e).log("Interrupted on watching namespace changes");
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        }
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        if (KeeperException.Code.OK.intValue() == rc) {
            log.info().attr("uri", uri).attr("children", children).log("Received updated logs");
            List<String> result = new ArrayList<String>(children.size());
            for (String s : children) {
                if (isReservedStreamName(s)) {
                    continue;
                }
                result.add(s);
            }
            for (NamespaceListener listener : listeners) {
                listener.onStreamsChanged(result.iterator());
            }
        } else {
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None) {
            if (event.getState() == Event.KeeperState.Expired) {
                scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
            }
            return;
        }
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            // watch namespace changes again.
            doWatchNamespaceChanges();
        }
    }
}
