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
package org.apache.bookkeeper.meta;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.meta.AbstractZkLedgerManager.ZK_CONNECT_BACKOFF_MS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.testing.executors.MockExecutorController;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.MockZooKeeperTestCase;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit test of {@link AbstractZkLedgerManager}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ AbstractZkLedgerManager.class, ZkUtils.class })
public class AbstractZkLedgerManagerTest extends MockZooKeeperTestCase {

    private ClientConfiguration conf;
    private AbstractZkLedgerManager ledgerManager;
    private ScheduledExecutorService scheduler;
    private MockExecutorController schedulerController;
    private LedgerMetadata metadata;
    private LedgerMetadataSerDe serDe;

    @Before
    public void setup() throws Exception {
        PowerMockito.mockStatic(Executors.class);

        super.setup();

        this.scheduler = PowerMockito.mock(ScheduledExecutorService.class);
        this.schedulerController = new MockExecutorController()
            .controlSubmit(scheduler)
            .controlSchedule(scheduler)
            .controlExecute(scheduler)
            .controlScheduleAtFixedRate(scheduler, 10);
        PowerMockito.when(Executors.newSingleThreadScheduledExecutor(any()))
            .thenReturn(scheduler);

        this.conf = new ClientConfiguration();
        this.ledgerManager = mock(
            AbstractZkLedgerManager.class,
            withSettings()
                .useConstructor(conf, mockZk)
                .defaultAnswer(CALLS_REAL_METHODS));
        List<BookieSocketAddress> ensemble = Lists.newArrayList(
                new BookieSocketAddress("192.0.2.1", 3181),
                new BookieSocketAddress("192.0.2.2", 3181),
                new BookieSocketAddress("192.0.2.3", 3181),
                new BookieSocketAddress("192.0.2.4", 3181),
                new BookieSocketAddress("192.0.2.5", 3181));
        this.metadata = LedgerMetadataBuilder.create()
            .withDigestType(DigestType.CRC32C).withPassword(new byte[0])
            .withEnsembleSize(5)
            .withMetadataFormatVersion(2)
            .withWriteQuorumSize(3)
            .withAckQuorumSize(3)
            .newEnsembleEntry(0L, ensemble)
            .withCreationTime(12345L).build();

        doAnswer(invocationOnMock -> {
            long ledgerId = invocationOnMock.getArgument(0);
            return String.valueOf(ledgerId);
        }).when(ledgerManager).getLedgerPath(anyLong());
        doAnswer(invocationOnMock -> {
            String ledgerStr = invocationOnMock.getArgument(0);
            return Long.parseLong(ledgerStr);
        }).when(ledgerManager).getLedgerId(anyString());

        // verify constructor
        assertEquals(ZKMetadataDriverBase.resolveZkLedgersRootPath(conf), ledgerManager.ledgerRootPath);
        assertSame(mockZk, ledgerManager.zk);
        assertSame(conf, ledgerManager.conf);
        assertSame(scheduler, ledgerManager.scheduler);

        this.serDe = new LedgerMetadataSerDe();
    }

    @After
    public void teardown() throws Exception {
        if (null != ledgerManager) {
            ledgerManager.close();

            // zookeeper is passed in, it should not be closed.
            verify(mockZk, times(0)).close();
            verify(scheduler, times(1)).shutdown();
        }
    }

    @Test
    public void testCreateLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);
        mockZkUtilsAsyncCreateFullPathOptimistic(
            ledgerStr, CreateMode.PERSISTENT,
            KeeperException.Code.OK.intValue(), ledgerStr
        );

        Versioned<LedgerMetadata> result = ledgerManager.createLedgerMetadata(ledgerId, metadata).get();

        assertEquals(new LongVersion(0), result.getVersion());
    }

    @Test
    public void testCreateLedgerMetadataNodeExists() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);
        mockZkUtilsAsyncCreateFullPathOptimistic(
            ledgerStr, CreateMode.PERSISTENT,
            KeeperException.Code.NODEEXISTS.intValue(), null);

        try {
            result(ledgerManager.createLedgerMetadata(ledgerId, metadata));
            fail("Should fail to create ledger metadata if the ledger already exists");
        } catch (Exception e) {
            assertTrue(e instanceof BKException);
            BKException bke = (BKException) e;
            assertEquals(Code.LedgerExistException, bke.getCode());
        }
    }

    @Test
    public void testCreateLedgerMetadataException() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);
        mockZkUtilsAsyncCreateFullPathOptimistic(
            ledgerStr, CreateMode.PERSISTENT,
            KeeperException.Code.CONNECTIONLOSS.intValue(), null);

        try {
            result(ledgerManager.createLedgerMetadata(ledgerId, metadata));
            fail("Should fail to create ledger metadata when encountering zookeeper exception");
        } catch (Exception e) {
            assertTrue(e instanceof BKException);
            BKException bke = (BKException) e;
            assertEquals(Code.ZKException, bke.getCode());
        }
    }

    @Test
    public void testRemoveLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);
        LongVersion version = new LongVersion(1234L);

        mockZkDelete(
            ledgerStr, (int) version.getLongVersion(),
            KeeperException.Code.OK.intValue());

        ledgerManager.removeLedgerMetadata(ledgerId, version).get();

        verify(mockZk, times(1))
            .delete(eq(ledgerStr), eq(1234), any(VoidCallback.class), eq(null));
    }

    @Test
    public void testRemoveLedgerMetadataVersionAny() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        mockZkDelete(
            ledgerStr, -1,
            KeeperException.Code.OK.intValue());

        ledgerManager.removeLedgerMetadata(ledgerId, Version.ANY).get();

        verify(mockZk, times(1))
            .delete(eq(ledgerStr), eq(-1), any(VoidCallback.class), eq(null));
    }

    @Test
    public void testRemoveLedgerMetadataVersionNew() throws Exception {
        testRemoveLedgerMetadataInvalidVersion(Version.NEW);
    }

    @Test
    public void testRemoveLedgerMetadataUnknownVersionType() throws Exception {
        Version version = mock(Version.class);
        testRemoveLedgerMetadataInvalidVersion(version);
    }

    private void testRemoveLedgerMetadataInvalidVersion(Version version) throws Exception {
        long ledgerId = System.currentTimeMillis();

        try {
            result(ledgerManager.removeLedgerMetadata(ledgerId, version));
            fail("Should fail to remove metadata if version is " + Version.NEW);
        } catch (BKException bke) {
            assertEquals(Code.MetadataVersionException, bke.getCode());
        }
    }

    @Test
    public void testRemoveLedgerMetadataNoNode() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);
        LongVersion version = new LongVersion(1234L);

        mockZkDelete(
            ledgerStr, (int) version.getLongVersion(),
            KeeperException.Code.NONODE.intValue());

        try {
            result(ledgerManager.removeLedgerMetadata(ledgerId, version));
        } catch (BKException bke) {
            fail("Should succeed");
        }

        verify(mockZk, times(1))
            .delete(eq(ledgerStr), eq(1234), any(VoidCallback.class), eq(null));
    }

    @Test
    public void testRemoveLedgerMetadataException() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);
        LongVersion version = new LongVersion(1234L);

        mockZkDelete(
            ledgerStr, (int) version.getLongVersion(),
            KeeperException.Code.CONNECTIONLOSS.intValue());

        try {
            result(ledgerManager.removeLedgerMetadata(ledgerId, version));
            fail("Should fail to remove metadata upon ZKException");
        } catch (BKException bke) {
            assertEquals(Code.ZKException, bke.getCode());
        }

        verify(mockZk, times(1))
            .delete(eq(ledgerStr), eq(1234), any(VoidCallback.class), eq(null));
    }

    @Test
    public void testRemoveLedgerMetadataHierarchical() throws Exception {
        HierarchicalLedgerManager hlm = new HierarchicalLedgerManager(conf, mockZk);
        testRemoveLedgerMetadataHierarchicalLedgerManager(hlm);
    }

    @Test
    public void testRemoveLedgerMetadataLongHierarchical() throws Exception {
        LongHierarchicalLedgerManager hlm = new LongHierarchicalLedgerManager(conf, mockZk);
        testRemoveLedgerMetadataHierarchicalLedgerManager(hlm);
    }

    private void testRemoveLedgerMetadataHierarchicalLedgerManager(AbstractZkLedgerManager lm) throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = lm.getLedgerPath(ledgerId);
        LongVersion version = new LongVersion(1234L);

        mockZkUtilsAsyncDeleteFullPathOptimistic(
            ledgerStr, (int) version.getLongVersion(),
            KeeperException.Code.OK.intValue());

        lm.removeLedgerMetadata(ledgerId, version).get();

        PowerMockito.verifyStatic(
            ZkUtils.class, times(1));
        ZkUtils.asyncDeleteFullPathOptimistic(
            eq(mockZk), eq(ledgerStr), eq(1234), any(VoidCallback.class), eq(ledgerStr));

    }

    @Test
    public void testReadLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkGetData(
            ledgerStr, false,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        Versioned<LedgerMetadata> readMetadata = result(ledgerManager.readLedgerMetadata(ledgerId));
        assertEquals(metadata, readMetadata.getValue());
        assertEquals(new LongVersion(1234), readMetadata.getVersion());

        verify(mockZk, times(1))
            .getData(eq(ledgerStr), eq(null), any(DataCallback.class), any());
    }

    @Test
    public void testReadLedgerMetadataNoNode() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        mockZkGetData(
            ledgerStr, false,
            KeeperException.Code.NONODE.intValue(), null, null);

        try {
            result(ledgerManager.readLedgerMetadata(ledgerId));
            fail("Should fail on reading ledger metadata if a ledger doesn't exist");
        } catch (BKException bke) {
            assertEquals(Code.NoSuchLedgerExistsException, bke.getCode());
        }

        verify(mockZk, times(1))
            .getData(eq(ledgerStr), eq(null), any(DataCallback.class), any());
    }

    @Test
    public void testReadLedgerMetadataException() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        mockZkGetData(
            ledgerStr, false,
            KeeperException.Code.CONNECTIONLOSS.intValue(), null, null);

        try {
            result(ledgerManager.readLedgerMetadata(ledgerId));
            fail("Should fail on reading ledger metadata if a ledger doesn't exist");
        } catch (BKException bke) {
            assertEquals(Code.ZKException, bke.getCode());
        }

        verify(mockZk, times(1))
            .getData(eq(ledgerStr), eq(null), any(DataCallback.class), any());
    }

    @Test
    public void testReadLedgerMetadataStatMissing() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        mockZkGetData(
            ledgerStr, false,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), null);

        try {
            result(ledgerManager.readLedgerMetadata(ledgerId));
            fail("Should fail on reading ledger metadata if a ledger doesn't exist");
        } catch (BKException bke) {
            assertEquals(Code.ZKException, bke.getCode());
        }

        verify(mockZk, times(1))
            .getData(eq(ledgerStr), eq(null), any(DataCallback.class), any());
    }

    @Test
    public void testReadLedgerMetadataDataCorrupted() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkGetData(
            ledgerStr, false,
            KeeperException.Code.OK.intValue(), new byte[0], stat);

        try {
            result(ledgerManager.readLedgerMetadata(ledgerId));
            fail("Should fail on reading ledger metadata if a ledger doesn't exist");
        } catch (BKException bke) {
            assertEquals(Code.ZKException, bke.getCode());
        }

        verify(mockZk, times(1))
            .getData(eq(ledgerStr), eq(null), any(DataCallback.class), any());
    }

    @Test
    public void testWriteLedgerMetadataSuccess() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1235);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkSetData(
            ledgerStr, serDe.serialize(metadata), 1234,
            KeeperException.Code.OK.intValue(), stat);

        Version v = ledgerManager.writeLedgerMetadata(ledgerId, metadata, new LongVersion(1234L)).get().getVersion();

        assertEquals(new LongVersion(1235L), v);

        verify(mockZk, times(1))
            .setData(eq(ledgerStr), any(byte[].class), eq(1234), any(StatCallback.class), any());
    }

    @Test
    public void testWriteLedgerMetadataBadVersion() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        mockZkSetData(
            ledgerStr, serDe.serialize(metadata), 1234,
            KeeperException.Code.BADVERSION.intValue(), null);

        try {
            result(ledgerManager.writeLedgerMetadata(ledgerId, metadata, new LongVersion(1234L)));
            fail("Should fail on writing ledger metadata if encountering bad version");
        } catch (BKException bke) {
            assertEquals(Code.MetadataVersionException, bke.getCode());
        }

        verify(mockZk, times(1))
            .setData(eq(ledgerStr), any(byte[].class), eq(1234), any(StatCallback.class), any());
    }

    @Test
    public void testWriteLedgerMetadataException() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        mockZkSetData(
            ledgerStr, serDe.serialize(metadata), 1234,
            KeeperException.Code.CONNECTIONLOSS.intValue(), null);

        try {
            result(ledgerManager.writeLedgerMetadata(ledgerId, metadata, new LongVersion(1234L)));
            fail("Should fail on writing ledger metadata if encountering zookeeper exceptions");
        } catch (BKException bke) {
            assertEquals(Code.ZKException, bke.getCode());
        }


        verify(mockZk, times(1))
            .setData(eq(ledgerStr), any(byte[].class), eq(1234), any(StatCallback.class), any());
    }

    @Test
    public void testWriteLedgerMetadataInvalidVersion() throws Exception {
        Version[] versions = new Version[] {
            Version.NEW,
            Version.ANY,
            mock(Version.class)
        };
        for (Version version : versions) {
            testWriteLedgerMetadataInvalidVersion(version);
        }
    }

    private void testWriteLedgerMetadataInvalidVersion(Version invalidVersion) throws Exception {
        long ledgerId = System.currentTimeMillis();

        try {
            result(ledgerManager.writeLedgerMetadata(ledgerId, metadata, invalidVersion));
            fail("Should fail on writing ledger metadata if an invalid version is provided.");
        } catch (BKException bke) {
            assertEquals(Code.MetadataVersionException, bke.getCode());
        }

        verify(mockZk, times(0))
            .setData(anyString(), any(byte[].class), anyInt(), any(StatCallback.class), any());
    }

    @Test
    public void testLedgerMetadataListener() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        LinkedBlockingQueue<LedgerMetadata> changes = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener = (ledgerId1, metadata) -> changes.add(metadata.getValue());

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        ledgerManager.registerLedgerMetadataListener(ledgerId, listener);

        // the listener will be notified with first get
        LedgerMetadata change1 = changes.take();
        assertEquals(metadata, change1);
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // the watcher is registered for receiving watched event
        assertTrue(watchers.containsKey(ledgerStr));
        Set<Watcher> watcherSet1 = watchers.get(ledgerStr);
        assertEquals(1, watcherSet1.size());
        Watcher registeredWatcher1 = watcherSet1.stream().findFirst().get();

        // mock get data to return an updated metadata
        when(stat.getVersion()).thenReturn(1235);
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        // notify the watcher event
        notifyWatchedEvent(
            EventType.NodeDataChanged, KeeperState.SyncConnected, ledgerStr);

        // the listener should receive an updated metadata
        LedgerMetadata change2 = changes.take();
        assertEquals(metadata, change2);
        verify(mockZk, times(2))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // after the listener receive an updated metadata, a new watcher should be registered
        // for subsequent changes again.
        assertTrue(watchers.containsKey(ledgerStr));
        Set<Watcher> watcherSet2 = watchers.get(ledgerStr);
        assertEquals(1, watcherSet2.size());
        Watcher registeredWatcher2 = watcherSet2.stream().findFirst().get();

        // zookeeper watchers are same, since there is only one giant watcher per ledger manager.
        assertSame(registeredWatcher1, registeredWatcher2);

        // verify scheduler
        verify(scheduler, times(2)).submit(any(Runnable.class));
        verify(scheduler, times(0))
            .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
    }

    @Test
    public void testLedgerMetadataListenerOnLedgerDeleted() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        LinkedBlockingQueue<Optional<LedgerMetadata>> changes = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener =
            (ledgerId1, metadata) -> changes.add(Optional.ofNullable(metadata != null ? metadata.getValue() : null));

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        ledgerManager.registerLedgerMetadataListener(ledgerId, listener);
        assertTrue(ledgerManager.listeners.containsKey(ledgerId));

        // the listener will be notified with first get
        LedgerMetadata change1 = changes.take().get();
        assertEquals(metadata, change1);
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // the watcher is registered for receiving watched event
        assertTrue(watchers.containsKey(ledgerStr));

        // mock get data to simulate an ledger is deleted
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.NONODE.intValue(), null, null);

        // notify the watcher event
        notifyWatchedEvent(
            EventType.NodeDataChanged, KeeperState.SyncConnected, ledgerStr);

        // the listener should be removed from listener set and not receive an updated metadata anymore
        Optional<LedgerMetadata> change2 = changes.take();
        assertFalse(change2.isPresent());
        assertFalse(ledgerManager.listeners.containsKey(ledgerId));

        // verify scheduler: the listener is only triggered once
        verify(scheduler, times(1)).submit(any(Runnable.class));
        verify(scheduler, times(0)).schedule(
            any(Runnable.class), anyLong(), any(TimeUnit.class));

        // no watcher is registered
        assertFalse(watchers.containsKey(ledgerStr));
    }

    @Test
    public void testLedgerMetadataListenerOnLedgerDeletedEvent() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        LinkedBlockingQueue<Optional<LedgerMetadata>> changes = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener =
            (ledgerId1, metadata) -> changes.add(
                    Optional.ofNullable(metadata != null ? metadata.getValue() : null));

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        ledgerManager.registerLedgerMetadataListener(ledgerId, listener);
        assertTrue(ledgerManager.listeners.containsKey(ledgerId));

        // the listener will be notified with first get
        LedgerMetadata change1 = changes.take().get();
        assertEquals(metadata, change1);
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // the watcher is registered for receiving watched event
        assertTrue(watchers.containsKey(ledgerStr));

        // notify the watcher event
        notifyWatchedEvent(
            EventType.NodeDeleted, KeeperState.SyncConnected, ledgerStr);

        // the listener should be removed from listener set and a null change is notified.
        Optional<LedgerMetadata> change2 = changes.take();
        assertFalse(change2.isPresent());
        // no more `getData` is called.
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());
        // listener is automatically unregistered after a ledger is deleted.
        assertFalse(ledgerManager.listeners.containsKey(ledgerId));
    }

    @Test
    public void testLedgerMetadataListenerOnRetries() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        LinkedBlockingQueue<LedgerMetadata> changes = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener = (ledgerId1, metadata) -> changes.add(metadata.getValue());

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());

        // fail the first get, so the ledger manager will retry get data again.
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.SESSIONEXPIRED.intValue(), null, null);

        ledgerManager.registerLedgerMetadataListener(ledgerId, listener);
        assertTrue(ledgerManager.listeners.containsKey(ledgerId));

        // the listener will not be notified with any updates
        assertNull(changes.poll());
        // an retry task is scheduled
        verify(scheduler, times(1))
            .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        // zookeeper is called once
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());
        // watcher is not registered since getData call is failed
        assertFalse(watchers.containsKey(ledgerStr));

        // mock get data to return a valid response
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        schedulerController.advance(Duration.ofMillis(ZK_CONNECT_BACKOFF_MS));

        // the listener will be notified with first get
        LedgerMetadata change = changes.take();
        assertEquals(metadata, change);
        verify(mockZk, times(2))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // watcher is registered after successfully `getData`
        assertTrue(watchers.containsKey(ledgerStr));
    }

    @Test
    public void testLedgerMetadataListenerOnSessionExpired() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        LinkedBlockingQueue<LedgerMetadata> changes = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener = (ledgerId1, metadata) -> changes.add(metadata.getValue());

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        ledgerManager.registerLedgerMetadataListener(ledgerId, listener);

        // the listener will be notified with first get
        LedgerMetadata change1 = changes.take();
        assertEquals(metadata, change1);
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // the watcher is registered for receiving watched event
        assertTrue(watchers.containsKey(ledgerStr));
        Set<Watcher> watcherSet1 = watchers.get(ledgerStr);
        assertEquals(1, watcherSet1.size());
        Watcher registeredWatcher1 = watcherSet1.stream().findFirst().get();

        // simulate session expired
        notifyWatchedEvent(
            EventType.None, KeeperState.Expired, ledgerStr);

        // ledger manager will retry to read metadata again
        LedgerMetadata change2 = changes.take();
        assertEquals(metadata, change2);
        verify(mockZk, times(2))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // the watcher is registered for receiving watched event
        assertTrue(watchers.containsKey(ledgerStr));
        Set<Watcher> watcherSet2 = watchers.get(ledgerStr);
        assertEquals(1, watcherSet2.size());
        Watcher registeredWatcher2 = watcherSet2.stream().findFirst().get();

        assertSame(registeredWatcher1, registeredWatcher2);
    }

    @Test
    public void testUnregisterLedgerMetadataListener() throws Exception {
        long ledgerId = System.currentTimeMillis();
        String ledgerStr = String.valueOf(ledgerId);

        LinkedBlockingQueue<LedgerMetadata> changes = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener = (ledgerId1, metadata) -> changes.add(metadata.getValue());

        Stat stat = mock(Stat.class);
        when(stat.getVersion()).thenReturn(1234);
        when(stat.getCtime()).thenReturn(metadata.getCtime());
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        ledgerManager.registerLedgerMetadataListener(ledgerId, listener);
        assertTrue(ledgerManager.listeners.containsKey(ledgerId));

        // the listener will be notified with first get
        LedgerMetadata change1 = changes.take();
        assertEquals(metadata, change1);
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());

        // the watcher is registered for receiving watched event
        assertTrue(watchers.containsKey(ledgerStr));
        Set<Watcher> watcherSet1 = watchers.get(ledgerStr);
        assertEquals(1, watcherSet1.size());
        Watcher registeredWatcher1 = watcherSet1.stream().findFirst().get();

        // mock get data to return an updated metadata
        when(stat.getVersion()).thenReturn(1235);
        mockZkGetData(
            ledgerStr, true,
            KeeperException.Code.OK.intValue(), serDe.serialize(metadata), stat);

        // unregister the listener
        ledgerManager.unregisterLedgerMetadataListener(ledgerId, listener);
        assertFalse(ledgerManager.listeners.containsKey(ledgerId));

        // notify the watcher event
        notifyWatchedEvent(
            EventType.NodeDataChanged, KeeperState.SyncConnected, ledgerStr);

        // since listener is already unregistered so no more `getData` is issued.
        assertNull(changes.poll());
        verify(mockZk, times(1))
            .getData(anyString(), any(Watcher.class), any(DataCallback.class), any());
    }

}
