/*
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
 */
package org.apache.bookkeeper.discover;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.discover.ZKRegistrationClient.ZK_CONNECT_BACKOFF_MS;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.ZKException;
import org.apache.bookkeeper.common.testing.executors.MockExecutorController;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.discover.ZKRegistrationClient.WatchTask;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Unit test of {@link RegistrationClient}.
 */
@Slf4j
public class TestZkRegistrationClient {

    @Rule
    public final TestName runtime = new TestName();

    private String ledgersPath;
    private String regPath;
    private String regReadonlyPath;
    private ZooKeeper zk;
    private ZKRegistrationClient zkRegistrationClient;
    private ScheduledExecutorService mockExecutor;
    private MockExecutorController controller;
    private final Map<String, Set<Watcher>> watchers = Maps.newHashMap();

    @Before
    public void setup() {
        this.ledgersPath = "/" + runtime.getMethodName();
        this.regPath = ledgersPath + "/" + AVAILABLE_NODE;
        this.regReadonlyPath = regPath + "/" + READONLY;
        this.zk = mock(ZooKeeper.class);
        this.mockExecutor = mock(ScheduledExecutorService.class);
        this.controller = new MockExecutorController()
            .controlExecute(mockExecutor)
            .controlSubmit(mockExecutor)
            .controlSchedule(mockExecutor)
            .controlScheduleAtFixedRate(mockExecutor, 10);
        this.zkRegistrationClient = new ZKRegistrationClient(
            zk,
            ledgersPath,
            mockExecutor
        );
    }

    private void registerWatcher(String path, Watcher watcher) {
        if (null == watcher) {
            return;
        }
        synchronized (watchers) {
            Set<Watcher> watcherSet = watchers.get(path);
            if (null == watcherSet) {
                watcherSet = Sets.newHashSet();
                watchers.put(path, watcherSet);
            }
            watcherSet.add(watcher);
        }
    }

    private void mockGetChildren(String path, Class<? extends Watcher> watcherCls,
                                 int rcToRespond,
                                 List<String> childrenToRespond,
                                 Stat statToRespond) {
        doAnswer(invocationOnMock -> {
            String p = invocationOnMock.getArgument(0);
            Watcher w = invocationOnMock.getArgument(1);
            Children2Callback callback = invocationOnMock.getArgument(2);
            Object ctx = invocationOnMock.getArgument(3);

            registerWatcher(p, w);

            callback.processResult(
                rcToRespond,
                p,
                ctx,
                childrenToRespond,
                statToRespond
            );

            return null;

        }).when(zk).getChildren(eq(path), any(watcherCls), any(Children2Callback.class), any());
    }

    private static Set<BookieSocketAddress> prepareNBookies(int num) {
        Set<BookieSocketAddress> bookies = Sets.newHashSet();
        for (int i = 0; i < num; i++) {
            bookies.add(new BookieSocketAddress("127.0.0.1", 3181 + i));
        }
        return bookies;
    }

    @Test
    public void testGetWritableBookies() throws Exception {
        Set<BookieSocketAddress> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();
        for (BookieSocketAddress address : addresses) {
            children.add(address.toString());
        }
        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        mockGetChildren(
            regPath, Watcher.class,
            Code.OK.intValue(), children, stat);

        Versioned<Set<BookieSocketAddress>> result =
            result(zkRegistrationClient.getWritableBookies());

        assertEquals(new LongVersion(1234), result.getVersion());
        assertTrue(Sets.difference(addresses, result.getValue()).isEmpty());
        assertTrue(Sets.difference(result.getValue(), addresses).isEmpty());
    }

    @Test
    public void testGetReadOnlyBookies() throws Exception {
        Set<BookieSocketAddress> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();
        for (BookieSocketAddress address : addresses) {
            children.add(address.toString());
        }
        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        mockGetChildren(
            regReadonlyPath, Watcher.class,
            Code.OK.intValue(), children, stat);

        Versioned<Set<BookieSocketAddress>> result =
            result(zkRegistrationClient.getReadOnlyBookies());

        assertEquals(new LongVersion(1234), result.getVersion());
        assertTrue(Sets.difference(addresses, result.getValue()).isEmpty());
        assertTrue(Sets.difference(result.getValue(), addresses).isEmpty());
    }

    @Test
    public void testGetWritableBookiesFailure() throws Exception {
        mockGetChildren(
            regPath, Watcher.class,
            Code.NONODE.intValue(), null, null);

        try {
            result(zkRegistrationClient.getWritableBookies());
            fail("Should fail to get writable bookies");
        } catch (ZKException zke) {
            // expected to throw zookeeper exception
        }
    }

    @Test
    public void testGetReadOnlyBookiesFailure() throws Exception {
        mockGetChildren(
            regReadonlyPath, Watcher.class,
            Code.NONODE.intValue(), null, null);

        try {
            result(zkRegistrationClient.getReadOnlyBookies());
            fail("Should fail to get writable bookies");
        } catch (ZKException zke) {
            // expected to throw zookeeper exception
        }
    }

    @Test
    public void testWatchWritableBookiesSuccess() throws Exception {
        testWatchBookiesSuccess(true);
    }

    @Test
    public void testWatchReadonlyBookiesSuccess() throws Exception {
        testWatchBookiesSuccess(false);
    }

    private void testWatchBookiesSuccess(boolean isWritable)
            throws Exception {
        LinkedBlockingQueue<Versioned<Set<BookieSocketAddress>>> updates =
            new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        Set<BookieSocketAddress> addresses = prepareNBookies(10);
        List<String> children = Lists.newArrayList();
        for (BookieSocketAddress address : addresses) {
            children.add(address.toString());
        }
        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);

        mockGetChildren(
            isWritable ? regPath : regReadonlyPath,
            WatchTask.class,
            Code.OK.intValue(), children, stat);

        if (isWritable) {
            result(zkRegistrationClient.watchWritableBookies(listener));
        }  else {
            result(zkRegistrationClient.watchReadOnlyBookies(listener));
        }

        Versioned<Set<BookieSocketAddress>> update = updates.take();
        assertEquals(new LongVersion(1234), update.getVersion());
        assertTrue(Sets.difference(addresses, update.getValue()).isEmpty());
        assertTrue(Sets.difference(update.getValue(), addresses).isEmpty());

        verify(zk, times(1))
            .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());

        log.info("Expire sessions");
        // simulate session expire
        Set<Watcher> watcherSet = watchers.remove(isWritable ? regPath : regReadonlyPath);
        assertNotNull(watcherSet);
        assertEquals(1, watcherSet.size());
        for (Watcher watcher : watcherSet) {
            watcher.process(new WatchedEvent(
                EventType.None,
                KeeperState.Expired,
                isWritable ? regPath : regReadonlyPath));
        }

        // if session expires, the watcher task will get into backoff state
        controller.advance(Duration.ofMillis(ZK_CONNECT_BACKOFF_MS));

        // the same updates returns, the getChildren calls increase to 2
        // but since there is no updates, so no notification is sent.
        verify(zk, times(2))
            .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());
        assertNull(updates.poll());

        // prepare for new watcher notification

        Set<BookieSocketAddress> newAddresses = prepareNBookies(20);
        List<String> newChildren = Lists.newArrayList();
        for (BookieSocketAddress address : newAddresses) {
            newChildren.add(address.toString());
        }
        Stat newStat = mock(Stat.class);
        when(newStat.getVersion()).thenReturn(1235);

        mockGetChildren(
            isWritable ? regPath : regReadonlyPath,
            WatchTask.class,
            Code.OK.intValue(), newChildren, newStat);

        // trigger watcher
        watcherSet = watchers.get(isWritable ? regPath : regReadonlyPath);
        assertNotNull(watcherSet);
        assertEquals(1, watcherSet.size());
        for (Watcher watcher : watcherSet) {
            watcher.process(new WatchedEvent(
                EventType.NodeChildrenChanged,
                KeeperState.SyncConnected,
                isWritable ? regPath : regReadonlyPath));
        }

        update = updates.take();
        assertEquals(new LongVersion(1235), update.getVersion());
        assertTrue(Sets.difference(newAddresses, update.getValue()).isEmpty());
        assertTrue(Sets.difference(update.getValue(), newAddresses).isEmpty());

        verify(zk, times(3))
            .getChildren(anyString(), any(Watcher.class), any(Children2Callback.class), any());
    }

}
