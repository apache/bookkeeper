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
package org.apache.bookkeeper.stream.storage.impl.store;

import static org.apache.bookkeeper.statelib.impl.mvcc.MVCCUtils.NOP_CMD;
import static org.apache.bookkeeper.stream.storage.impl.store.MVCCStoreFactoryImpl.normalizedName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.bookkeeper.statelib.impl.rocksdb.checkpoint.fs.FSCheckpointManager;
import org.apache.bookkeeper.stream.storage.StorageResources;
import org.apache.bookkeeper.stream.storage.StorageResourcesSpec;
import org.apache.bookkeeper.stream.storage.conf.StorageConfiguration;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit test of {@link MVCCStoreFactoryImpl}.
 */
@Slf4j
public class MVCCStoreFactoryImplTest {

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    private Namespace namespace;
    private File[] storeDirs;
    private StorageResources resources;
    private MVCCStoreFactoryImpl factory;

    private final CompositeConfiguration compConf =
        new CompositeConfiguration();
    private final StorageConfiguration storageConf =
        new StorageConfiguration(compConf);

    @Before
    public void setup() throws IOException {
        this.namespace = mock(Namespace.class);

        DistributedLogManager dlm = mock(DistributedLogManager.class);
        when(dlm.asyncClose()).thenReturn(FutureUtils.Void());
        when(namespace.openLog(anyString())).thenReturn(dlm);
        AsyncLogWriter logWriter = mock(AsyncLogWriter.class);
        when(dlm.openAsyncLogWriter()).thenReturn(FutureUtils.value(logWriter));
        when(dlm.openAsyncLogWriter(any())).thenReturn(FutureUtils.value(logWriter));
        when(logWriter.getLastTxId()).thenReturn(-1L);
        DLSN dlsn = new DLSN(0L, 0L, 0L);
        when(logWriter.write(any(LogRecord.class))).thenReturn(FutureUtils.value(dlsn));
        when(logWriter.asyncClose()).thenReturn(FutureUtils.Void());
        AsyncLogReader logReader = mock(AsyncLogReader.class);
        when(dlm.openAsyncLogReader(anyLong())).thenReturn(FutureUtils.value(logReader));
        when(logReader.asyncClose()).thenReturn(FutureUtils.Void());
        LogRecordWithDLSN record = new LogRecordWithDLSN(
            dlsn, 0L, NOP_CMD.toByteArray(), 0L);
        when(logReader.readNext()).thenReturn(FutureUtils.value(record));

        int numDirs = 3;
        this.storeDirs = new File[numDirs];
        for (int i = 0; i < numDirs; i++) {
            storeDirs[i] = testDir.newFolder("test-" + i);
        }

        this.resources = StorageResources.create(
            StorageResourcesSpec.builder()
                .numCheckpointThreads(3)
                .numIOReadThreads(3)
                .numIOWriteThreads(3)
                .build());
        this.factory = new MVCCStoreFactoryImpl(
            () -> namespace,
            () -> new FSCheckpointManager(new File(storeDirs[0], "checkpoints")),
            storeDirs,
            resources,
            false, storageConf);
    }

    @Test
    public void testOpenStore() throws Exception {
        long scId = System.currentTimeMillis();
        long streamId = scId + 1;
        long rangeId = streamId + 1;

        try (MVCCAsyncStore<byte[], byte[]> store = FutureUtils.result(
            factory.openStore(scId, streamId, rangeId, 0))) {

            log.info("Open store (scId = {}, streamId = {}, rangeId = {}) to test",
                scId, streamId, rangeId);

            String storeName = String.format(
                "%s/%s/%s",
                normalizedName(scId),
                normalizedName(streamId),
                normalizedName(rangeId));
            assertEquals(storeName, store.name());

            File localStoreDir = Paths.get(
                storeDirs[(int) (streamId % storeDirs.length)].getAbsolutePath(),
                "ranges",
                normalizedName(scId),
                normalizedName(streamId),
                normalizedName(rangeId)
            ).toFile();
            assertEquals(localStoreDir, store.spec().getLocalStateStoreDir());

            String streamName = MVCCStoreFactoryImpl.streamName(scId, streamId, rangeId);
            assertEquals(streamName, store.spec().getStream());

            assertTrue(store.spec().getKeyCoder() instanceof ByteArrayCoder);
            assertTrue(store.spec().getValCoder() instanceof ByteArrayCoder);
            assertSame(
                factory.writeIOScheduler().chooseThread(streamId),
                store.spec().getWriteIOScheduler());
            assertSame(
                factory.readIOScheduler().chooseThread(streamId),
                store.spec().getReadIOScheduler());
            assertSame(
                factory.checkpointScheduler().chooseThread(streamId),
                store.spec().getCheckpointIOScheduler());
            assertTrue(store.spec().getCheckpointStore() instanceof FSCheckpointManager);
            assertEquals(Duration.ofMinutes(15), store.spec().getCheckpointDuration());
        }
    }

}
