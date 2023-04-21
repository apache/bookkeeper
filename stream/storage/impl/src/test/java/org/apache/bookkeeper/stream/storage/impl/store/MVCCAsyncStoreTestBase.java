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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import org.apache.bookkeeper.common.coder.ByteArrayCoder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.statelib.StateStores;
import org.apache.bookkeeper.statelib.api.StateStoreSpec;
import org.apache.bookkeeper.statelib.api.mvcc.MVCCAsyncStore;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

/**
 * An abstract test base that provides a mvcc store.
 */
public abstract class MVCCAsyncStoreTestBase {

    private static Namespace mockNamespace() throws Exception {
        Namespace namespace = mock(Namespace.class);
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
        return namespace;
    }

    @Rule
    public TestName name = new TestName();
    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();
    @Rule
    public final Timeout globalTimeout = Timeout.seconds(10);

    protected Namespace namespace;
    protected MVCCAsyncStore<byte[], byte[]> store;
    protected OrderedScheduler scheduler;

    @Before
    public void setUp() throws Exception {
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-root-range-store")
            .numThreads(1)
            .build();

        namespace = mockNamespace();

        File localStateDir = testDir.newFolder(name.getMethodName());
        StateStoreSpec spec = StateStoreSpec.builder()
            .name(name.getMethodName())
            .keyCoder(ByteArrayCoder.of())
            .valCoder(ByteArrayCoder.of())
            .localStateStoreDir(localStateDir)
            .stream(name.getMethodName())
            .writeIOScheduler(scheduler.chooseThread())
            .readIOScheduler(scheduler.chooseThread())
            .checkpointIOScheduler(null)
            .build();
        store = StateStores.mvccKvBytesStoreSupplier(() -> namespace).get();
        FutureUtils.result(store.init(spec));

        doSetup();
    }

    protected abstract void doSetup() throws Exception;

    @After
    public void tearDown() throws Exception {
        doTeardown();

        store.close();

        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    protected abstract void doTeardown() throws Exception;

}
