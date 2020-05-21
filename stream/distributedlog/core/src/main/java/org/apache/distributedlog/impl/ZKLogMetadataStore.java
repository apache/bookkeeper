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
package org.apache.distributedlog.impl;

import static org.apache.distributedlog.util.DLUtils.isReservedStreamName;

import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.commons.lang.StringUtils;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.metadata.LogMetadataStore;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;


/**
 * ZooKeeper based log metadata store.
 */
public class ZKLogMetadataStore implements LogMetadataStore {

    final URI namespace;
    final Optional<URI> nsOptional;
    final ZooKeeperClient zkc;
    final ZKNamespaceWatcher nsWatcher;

    public ZKLogMetadataStore(
            DistributedLogConfiguration conf,
            URI namespace,
            ZooKeeperClient zkc,
            OrderedScheduler scheduler) {
        this.namespace = namespace;
        this.nsOptional = Optional.of(this.namespace);
        this.zkc = zkc;
        this.nsWatcher = new ZKNamespaceWatcher(conf, namespace, zkc, scheduler);
    }

    @Override
    public CompletableFuture<URI> createLog(String logName) {
        return FutureUtils.value(namespace);
    }

    @Override
    public CompletableFuture<Optional<URI>> getLogLocation(String logName) {
        return FutureUtils.value(nsOptional);
    }

    @Override
    public CompletableFuture<Iterator<String>> getLogs(String logNamePrefix) {
        final CompletableFuture<Iterator<String>> promise = new CompletableFuture<Iterator<String>>();
        final String nsRootPath;
        if (StringUtils.isEmpty(logNamePrefix)) {
            nsRootPath = namespace.getPath();
        } else {
            nsRootPath = namespace.getPath() + "/" + logNamePrefix;
        }
        try {
            final ZooKeeper zk = zkc.get();
            zk.sync(nsRootPath, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int syncRc, String syncPath, Object ctx) {
                    if (KeeperException.Code.OK.intValue() == syncRc) {
                        zk.getChildren(nsRootPath, false, new AsyncCallback.Children2Callback() {
                            @Override
                            public void processResult(int rc, String path, Object ctx,
                                                      List<String> children, Stat stat) {
                                if (KeeperException.Code.OK.intValue() == rc) {
                                    List<String> results = Lists.newArrayListWithExpectedSize(children.size());
                                    for (String child : children) {
                                        if (!isReservedStreamName(child)) {
                                            results.add(child);
                                        }
                                    }
                                    promise.complete(results.iterator());
                                } else if (KeeperException.Code.NONODE.intValue() == rc) {
                                    List<String> streams = Lists.newLinkedList();
                                    promise.complete(streams.iterator());
                                } else {
                                    promise.completeExceptionally(new ZKException("Error reading namespace "
                                            + nsRootPath, KeeperException.Code.get(rc)));
                                }
                            }
                        }, null);
                    } else if (KeeperException.Code.NONODE.intValue() == syncRc) {
                        List<String> streams = Lists.newLinkedList();
                        promise.complete(streams.iterator());
                    } else {
                        promise.completeExceptionally(new ZKException("Error reading namespace " + nsRootPath,
                                KeeperException.Code.get(syncRc)));
                    }
                }
            }, null);
            zkc.get();
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.completeExceptionally(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            promise.completeExceptionally(e);
        }
        return promise;
    }

    @Override
    public void registerNamespaceListener(NamespaceListener listener) {
        this.nsWatcher.registerListener(listener);
    }
}
