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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the subtree cache.
 */
public class SubTreeCacheTest {
    class TestTreeProvider implements SubTreeCache.TreeProvider {
        class Node {
            Watcher watcher = null;
            public Map<String, Node> children = new HashMap<>();
        }

        final Node root = new Node();

        Node getNode(String path) throws KeeperException {
            String[] pathSegments = path.split("/");
            Node cur = root;
            for (String segment : pathSegments) {
                if (segment.length() == 0) {
                    continue; // ignore leading empty one for leading /
                }
                if (cur.children.containsKey(segment)) {
                    cur = cur.children.get(segment);
                } else {
                    throw KeeperException.create(KeeperException.Code.NONODE);
                }
            }
            return cur;
        }

        @Override
        public List<String> getChildren(
                String path, Watcher watcher) throws InterruptedException, KeeperException {
            Node node = getNode(path);

            /* Enforce only one live watch per node */
            Assert.assertTrue(null == node.watcher);

            node.watcher = watcher;
            return new ArrayList<String>(node.children.keySet());
        }

        public void createNode(String path) throws KeeperException {
            String[] segments = path.split("/");
            if (segments.length == 0) {
                throw KeeperException.create(KeeperException.Code.NONODE);
            }
            String child = segments[segments.length - 1];
            String[] parentSegments = Arrays.copyOfRange(segments, 0, segments.length - 1);
            Node parent = getNode(String.join("/", parentSegments));
            if (parent.children.containsKey(child)) {
                throw KeeperException.create(KeeperException.Code.NODEEXISTS);
            } else {
                parent.children.put(child, new Node());
                if (null != parent.watcher) {
                    parent.watcher.process(
                            new WatchedEvent(
                                    Watcher.Event.EventType.NodeCreated,
                                    Watcher.Event.KeeperState.SyncConnected,
                                    path));
                    parent.watcher = null;
                }
            }
        }

        public void removeNode(String path) throws KeeperException {
            String[] segments = path.split("/");
            if (segments.length == 0) {
                throw KeeperException.create(KeeperException.Code.NONODE);
            }
            String child = segments[segments.length - 1];
            String[] parentSegments = Arrays.copyOfRange(segments, 0, segments.length - 1);
            String parentPath = String.join("/", parentSegments);
            Node parent = getNode(parentPath);
            if (!parent.children.containsKey(child)) {
                throw KeeperException.create(KeeperException.Code.NONODE);
            } else {
                Node cNode = parent.children.get(child);
                if (!cNode.children.isEmpty()) {
                    throw KeeperException.create(KeeperException.Code.NOTEMPTY);
                } else {
                    if (null != cNode.watcher) {
                        cNode.watcher.process(
                                new WatchedEvent(
                                        Watcher.Event.EventType.NodeChildrenChanged,
                                        Watcher.Event.KeeperState.SyncConnected,
                                        path));
                        cNode.watcher = null;
                    }
                    if (null != parent.watcher) {
                        parent.watcher.process(
                                new WatchedEvent(
                                        Watcher.Event.EventType.NodeDeleted,
                                        Watcher.Event.KeeperState.SyncConnected,
                                        parentPath));
                        parent.watcher = null;
                    }
                    parent.children.remove(child);
                }
            }
        }
    }

    TestTreeProvider tree = new TestTreeProvider();
    SubTreeCache cache = new SubTreeCache(tree);

    class TestWatch implements Watcher {
        boolean fired = false;

        @Override
        public void process(WatchedEvent event) {
            fired = true;
        }

        public boolean getFired() {
            return fired;
        }
    }

    TestWatch setWatch() {
        TestWatch watch = new TestWatch();
        cache.registerWatcher(watch);
        return watch;
    }

    void assertFired(TestWatch watch) {
        Assert.assertTrue(watch.getFired());
    }

    void assertNotFired(TestWatch watch) {
        Assert.assertFalse(watch.getFired());
    }

    class TestWatchGuard extends TestWatch implements AutoCloseable {
        SubTreeCache.WatchGuard guard;

        void setGuard(SubTreeCache.WatchGuard guard) {
            this.guard = guard;
        }

        @Override
        public void close() throws Exception {
            guard.close();
        }
    }

    TestWatchGuard setWatchWithGuard() {
        TestWatchGuard watch = new TestWatchGuard();
        watch.setGuard(cache.registerWatcherWithGuard(watch));
        return watch;
    }

    void readAssertChildren(String path, String[] children) throws KeeperException, InterruptedException {
        SortedSet<String> shouldBe = new TreeSet<String>(Arrays.asList(children));
        List<String> returned = cache.getChildren(path);
        SortedSet<String> is = new TreeSet<String>(returned);
        returned.clear(); // trip up implementations which return an internal reference
        Assert.assertEquals(shouldBe, is);
    }

    @Before
    public void setUp() throws Exception {
        String[] preCreate =
                {"/a"
                        , "/a/a"
                        , "/a/a/a"
                        , "/a/a/b"
                        , "/a/b"
                        , "/a/c"
                        , "/b"
                        , "/b/a"
                };
        for (String path : preCreate) {
            tree.createNode(path);
        }
    }

    @Test
    public void testNoUpdate() throws Exception {
        TestWatch watch = setWatch();
        readAssertChildren("/a/a", new String[]{"a", "b"});
        assertNotFired(watch);
    }

    @Test
    public void testSingleCreate() throws Exception {
        TestWatch watch = setWatch();
        readAssertChildren("/a/a", new String[]{"a", "b"});
        tree.createNode("/a/a/c");
        assertFired(watch);
    }

    @Test
    public void testSingleRemoval() throws Exception {
        TestWatch watch = setWatch();
        readAssertChildren("/a/a", new String[]{"a", "b"});
        tree.removeNode("/a/a/b");
        assertFired(watch);
    }

    @Test
    public void testCancelation() throws Exception {
        TestWatch watch = setWatch();
        readAssertChildren("/a/a", new String[]{"a", "b"});
        cache.cancelWatcher(watch);
        tree.createNode("/a/a/c");
        assertNotFired(watch);
    }

    @Test
    public void testGuardCancelation() throws Exception {
        TestWatch watch;
        try (TestWatchGuard guard = setWatchWithGuard()) {
            readAssertChildren("/a/a", new String[]{"a", "b"});
            watch = guard;
        }
        tree.createNode("/a/a/c");
        assertNotFired(watch);
    }

    @Test
    public void testGuardCancelationExceptional() throws Exception {
        TestWatch watch = null;
        try (TestWatchGuard guard = setWatchWithGuard()) {
            watch = guard;
            readAssertChildren("/z/a", new String[]{});
        } catch (Exception e) {
        }
        tree.createNode("/a/a/c");
        assertNotFired(watch);
    }

    @Test
    public void testDuplicateWatch() throws Exception {
        try (TestWatchGuard watch = setWatchWithGuard()) {
            readAssertChildren("/a/a", new String[]{"a", "b"});
        }
        try (TestWatchGuard watch = setWatchWithGuard()) {
            readAssertChildren("/a/a", new String[]{"a", "b"});
            assertNotFired(watch);
            tree.createNode("/a/a/e");
            assertFired(watch);
        }
    }

    @Test(expected = NoNodeException.class)
    public void testNoNode() throws Exception {
        try (TestWatchGuard watch = setWatchWithGuard()) {
            readAssertChildren("/z/a", new String[]{});
        }
    }

    @Test
    public void testRemoveEmptyNode() throws Exception {
        try (TestWatchGuard watch = setWatchWithGuard()) {
            readAssertChildren("/a/a/a", new String[]{});
            tree.removeNode("/a/a/a");
            assertFired(watch);
        }
    }

    @Test
    public void doubleWatch() throws Exception {
        try (TestWatchGuard watch1 = setWatchWithGuard()) {
            readAssertChildren("/a/a", new String[]{"a", "b"});
            try (TestWatchGuard watch2 = setWatchWithGuard()) {
                tree.createNode("/a/a/e");
                assertFired(watch1);
                readAssertChildren("/a/b", new String[]{});
                tree.createNode("/a/b/e");
                assertFired(watch2);
            }
        }
    }

    @Test
    public void sequentialWatch() throws Exception {
        try (TestWatchGuard watch = setWatchWithGuard()) {
            readAssertChildren("/a/a", new String[]{"a", "b"});
            tree.removeNode("/a/a/a");
            assertFired(watch);
        }
        try (TestWatchGuard watch = setWatchWithGuard()) {
            readAssertChildren("/a/a", new String[]{"b"});
            tree.removeNode("/a/a/b");
            assertFired(watch);
        }
    }
}
