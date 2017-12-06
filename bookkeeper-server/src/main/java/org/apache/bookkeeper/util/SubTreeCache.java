/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Caching layer for traversing and monitoring changes on a znode subtree.
 *
 * <p>ZooKeeper does not provide a way to perform a recursive watch on a subtree.
 * In order to detect changes to a subtree, we need to maintain a
 * cache of nodes which have been listed and have not changed since.  This would
 * mirror the set of nodes with live watches in ZooKeeper (since we can't
 * cancel them at the moment).
 *
 * <p>In order to avoid having to pre-read the whole subtree up front, we'll weaken
 * the guarantee to only require firing the watcher for updates on nodes read since
 * the watcher was registered which happened after the read.  We'll also permit
 * spurious events elsewhere in the tree to avoid having to distinguish between
 * nodes which were read before and after a watch was established.
 *
 * <p>Finally, we'll allow (require, even) the user to cancel a registered watcher
 * once no longer interested.
 */
public class SubTreeCache {
    private static final Logger LOG = LoggerFactory.getLogger(SubTreeCache.class);

    /**
     * A tree provider.
     */
    public interface TreeProvider {
        List<String> getChildren(
                String path, Watcher watcher) throws InterruptedException, KeeperException;
    }

    private class SubTreeNode implements Watcher {
        String path;
        private List<String> children;

        SubTreeNode(String path) {
            this.path = path;
        }

        private void setChildren(List<String> children) {
            this.children = children;
        }

        @Override
        public void process(WatchedEvent event) {
            synchronized (SubTreeCache.this) {
                handleEvent(event);
                cachedNodes.remove(path);
            }
        }

        private List<String> getChildren() {
            return new ArrayList<String>(children);
        }
    }

    TreeProvider provider;
    Set<Watcher> pendingWatchers = new HashSet<>();
    Map<String, SubTreeNode> cachedNodes = new HashMap<>();

    public SubTreeCache(TreeProvider provider) {
        this.provider = provider;
    }

    private synchronized void handleEvent(WatchedEvent event) {
        Set<Watcher> toReturn = pendingWatchers;
        for (Watcher watcher: pendingWatchers) {
            watcher.process(event);
        }
        pendingWatchers.clear();
    }


    /**
     * Returns children of node.
     *
     * @param path Path of which to get children
     * @return Children of path
     */
    public synchronized List<String> getChildren(String path) throws KeeperException, InterruptedException {
        SubTreeNode node = cachedNodes.get(path);
        if (null == node) {
            node = new SubTreeNode(path);
            node.setChildren(provider.getChildren(path, node));
            cachedNodes.put(path, node);
        }
        return node.getChildren();
    }

    /**
     * Register a watcher.
     *
     * <p>See class header for semantics.
     *
     * @param watcher watcher to register
     */
    public synchronized void registerWatcher(Watcher watcher) {
        pendingWatchers.add(watcher);
    }

    /**
     * Cancel a watcher (noop if not registered or already fired).
     *
     * @param watcher Watcher object to cancel
     */
    public synchronized void cancelWatcher(Watcher watcher) {
        pendingWatchers.remove(watcher);
    }

    /**
     * A watch guard.
     */
    public class WatchGuard implements AutoCloseable {
        final Watcher w;

        WatchGuard(Watcher w) {
            this.w = w;
        }

        @Override
        public void close() {
            cancelWatcher(w);
        }
    }

    /**
     * Register watcher and get interest guard object which can be used with try-with-resources.
     *
     * <p>It's important not to leak watchers into this structure.  The returned WatchGuard
     * can be used to ensure that the watch is unregistered upon exiting a scope.
     *
     * @param watcher Watcher to register
     */
    public synchronized WatchGuard registerWatcherWithGuard(Watcher watcher) {
        registerWatcher(watcher);
        return new WatchGuard(watcher);
    }
}
