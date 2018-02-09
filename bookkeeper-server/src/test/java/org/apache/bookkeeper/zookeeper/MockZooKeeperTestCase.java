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
package org.apache.bookkeeper.zookeeper;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Maps;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.client.api.BKException.Code;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.powermock.api.mockito.PowerMockito;

/**
 * A test base that provides mocked zookeeper.
 */
public abstract class MockZooKeeperTestCase {

    protected final ConcurrentMap<String, Set<Watcher>> watchers = Maps.newConcurrentMap();
    protected ZooKeeper mockZk;

    protected void setup() throws Exception {
        this.mockZk = mock(ZooKeeper.class);

        PowerMockito.mockStatic(ZkUtils.class);
    }

    private void addWatcher(String path, Watcher watcher) {
        if (null == watcher) {
            return;
        }
        Set<Watcher> watcherSet = watchers.get(path);
        if (null == watcherSet) {
            watcherSet = new HashSet<>();
            watchers.put(path, watcherSet);
        }
        watcherSet.add(watcher);
    }

    protected void mockZkUtilsAsyncCreateFullPathOptimistic(
        String expectedLedgerPath,
        CreateMode expectedCreateMode,
        int retCode,
        String retCreatedZnodeName
    ) throws Exception {

        PowerMockito.doAnswer(invocationOnMock -> {
            String path = invocationOnMock.getArgument(1);
            StringCallback callback = invocationOnMock.getArgument(5);
            Object ctx = invocationOnMock.getArgument(6);

            callback.processResult(
                retCode, path, ctx, retCreatedZnodeName);
            return null;
        }).when(
            ZkUtils.class,
            "asyncCreateFullPathOptimistic",
            eq(mockZk),
            eq(expectedLedgerPath),
            any(byte[].class),
            anyList(),
            eq(expectedCreateMode),
            any(StringCallback.class),
            any());

    }

    protected void mockZkDelete(
        String expectedLedgerPath,
        int expectedVersion,
        int retCode
    ) throws Exception {

        doAnswer(invocationOnMock -> {
            String path = invocationOnMock.getArgument(0);
            VoidCallback callback = invocationOnMock.getArgument(2);
            Object ctx = invocationOnMock.getArgument(3);

            callback.processResult(
                retCode, path, ctx
            );

            return null;
        }).when(mockZk).delete(
            eq(expectedLedgerPath),
            eq(expectedVersion),
            any(VoidCallback.class),
            any());

    }

    protected void mockZkUtilsAsyncDeleteFullPathOptimistic(
        String expectedLedgerPath,
        int expectedZnodeVersion,
        int retCode
    ) throws Exception {

        PowerMockito.doAnswer(invocationOnMock -> {
            String path = invocationOnMock.getArgument(1);
            VoidCallback callback = invocationOnMock.getArgument(3);

            callback.processResult(
                retCode, path, null);
            return null;
        }).when(
            ZkUtils.class,
            "asyncDeleteFullPathOptimistic",
            eq(mockZk),
            eq(expectedLedgerPath),
            eq(expectedZnodeVersion),
            any(VoidCallback.class),
            eq(expectedLedgerPath));

    }

    protected void mockZkGetData(
        String expectedLedgerPath,
        boolean expectedWatcher,
        int retCode,
        byte[] retData,
        Stat retStat
    ) throws Exception {

        doAnswer(invocationOnMock -> {
            String path = invocationOnMock.getArgument(0);
            Watcher watcher = invocationOnMock.getArgument(1);
            DataCallback callback = invocationOnMock.getArgument(2);
            Object ctx = invocationOnMock.getArgument(3);

            if (Code.OK == retCode) {
                addWatcher(path, watcher);
            }

            callback.processResult(
                retCode, path, ctx, retData, retStat
            );

            return null;
        }).when(mockZk).getData(
            eq(expectedLedgerPath),
            expectedWatcher ? any(Watcher.class) : eq(null),
            any(DataCallback.class),
            any());

    }

    protected void mockZkSetData(
        String expectedLedgerPath,
        byte[] expectedBytes,
        int expectedVersion,
        int retCode,
        Stat retStat
    ) throws Exception {

        doAnswer(invocationOnMock -> {
            String path = invocationOnMock.getArgument(0);
            StatCallback callback = invocationOnMock.getArgument(3);
            Object ctx = invocationOnMock.getArgument(4);

            callback.processResult(
                retCode, path, ctx, retStat
            );

            return null;
        }).when(mockZk).setData(
            eq(expectedLedgerPath),
            eq(expectedBytes),
            eq(expectedVersion),
            any(StatCallback.class),
            any());

    }

    protected boolean notifyWatchedEvent(EventType eventType,
                                         KeeperState keeperState,
                                         String path) {
        Set<Watcher> watcherSet = watchers.remove(path);
        if (null == watcherSet) {
            return false;
        }
        WatchedEvent event = new WatchedEvent(
            eventType, keeperState, path);
        for (Watcher watcher : watcherSet) {
            watcher.process(event);
        }
        return true;
    }

}
