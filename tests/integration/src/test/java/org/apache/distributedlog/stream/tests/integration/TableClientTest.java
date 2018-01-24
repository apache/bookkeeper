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
package org.apache.distributedlog.stream.tests.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.api.StorageClient;
import org.apache.distributedlog.api.kv.PTable;
import org.apache.distributedlog.api.kv.Txn;
import org.apache.distributedlog.api.kv.op.CompareResult;
import org.apache.distributedlog.api.kv.op.OpType;
import org.apache.distributedlog.api.kv.options.DeleteOption;
import org.apache.distributedlog.api.kv.options.DeleteOptionBuilder;
import org.apache.distributedlog.api.kv.options.OptionFactory;
import org.apache.distributedlog.api.kv.options.PutOption;
import org.apache.distributedlog.api.kv.options.PutOptionBuilder;
import org.apache.distributedlog.api.kv.options.RangeOption;
import org.apache.distributedlog.api.kv.options.RangeOptionBuilder;
import org.apache.distributedlog.api.kv.result.DeleteResult;
import org.apache.distributedlog.api.kv.result.KeyValue;
import org.apache.distributedlog.api.kv.result.PutResult;
import org.apache.distributedlog.api.kv.result.RangeResult;
import org.apache.distributedlog.api.kv.result.Result;
import org.apache.distributedlog.api.kv.result.TxnResult;
import org.apache.distributedlog.clients.StorageClientBuilder;
import org.apache.distributedlog.clients.admin.StorageAdminClient;
import org.apache.distributedlog.clients.config.StorageClientSettings;
import org.apache.distributedlog.clients.impl.kv.option.OptionFactoryImpl;
import org.apache.distributedlog.stream.proto.NamespaceConfiguration;
import org.apache.distributedlog.stream.proto.NamespaceProperties;
import org.apache.distributedlog.stream.proto.StreamConfiguration;
import org.apache.distributedlog.stream.proto.StreamProperties;
import org.apache.distributedlog.stream.proto.common.Endpoint;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Integration test for table service.
 */
@Slf4j
public class TableClientTest extends StorageServerTestBase {

    @Rule
    public final TestName testName = new TestName();

    private final String namespace = "test_namespace";
    private OrderedScheduler scheduler;
    private StorageAdminClient adminClient;
    private StorageClient storageClient;
    private final OptionFactory<ByteBuf> optionFactory = new OptionFactoryImpl<>();

    @Override
    protected void doSetup() throws Exception {
        scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("table-client-test")
            .numThreads(1)
            .build();
        StorageClientSettings settings = StorageClientSettings.newBuilder()
            .addEndpoints(cluster.getRpcEndpoints().toArray(new Endpoint[cluster.getRpcEndpoints().size()]))
            .usePlaintext(true)
            .build();
        String namespace = "test_namespace";
        adminClient = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .buildAdmin();
        storageClient = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .withNamespace(namespace)
            .build();
    }

    @Override
    protected void doTeardown() throws Exception {
        if (null != adminClient) {
            adminClient.close();
        }
        if (null != storageClient) {
            storageClient.close();
        }
        if (null != scheduler) {
            scheduler.shutdown();
        }
    }

    private static ByteBuf getLKey(int i) {
        return Unpooled.wrappedBuffer(String.format("test-lkey-%06d", i).getBytes(UTF_8));
    }

    private static ByteBuf getValue(int i) {
        return Unpooled.wrappedBuffer(String.format("test-val-%06d", i).getBytes(UTF_8));
    }

    @Test
    public void testTableAPI() throws Exception {
        // Create a namespace
        NamespaceConfiguration nsConf = NamespaceConfiguration.newBuilder()
            .setDefaultStreamConf(DEFAULT_STREAM_CONF)
            .build();
        NamespaceProperties nsProps = FutureUtils.result(adminClient.createNamespace(namespace, nsConf));
        assertEquals(namespace, nsProps.getNamespaceName());
        assertEquals(nsConf.getDefaultStreamConf(), nsProps.getDefaultStreamConf());

        // Create a stream
        String streamName = testName.getMethodName() + "_stream";
        StreamConfiguration streamConf = StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
            .build();
        StreamProperties streamProps = FutureUtils.result(
            adminClient.createStream(namespace, streamName, streamConf));
        assertEquals(streamName, streamProps.getStreamName());
        assertEquals(streamConf, streamProps.getStreamConf());

        // Open the table
        PTable<ByteBuf, ByteBuf> table = FutureUtils.result(storageClient.openPTable(streamName));
        byte[] rKey = "routing-key".getBytes(UTF_8);
        byte[] lKey = "testing-key".getBytes(UTF_8);
        byte[] value = "testing-value".getBytes(UTF_8);

        // put first key
        ByteBuf rKeyBuf = Unpooled.wrappedBuffer(rKey);
        ByteBuf lKeyBuf = Unpooled.wrappedBuffer(lKey);
        ByteBuf valBuf = Unpooled.wrappedBuffer(value);

        try (PutOptionBuilder<ByteBuf> optionBuilder = optionFactory.newPutOption().prevKv(true)) {
            try (PutOption<ByteBuf> option = optionBuilder.build()) {
                try (PutResult<ByteBuf, ByteBuf> putResult = FutureUtils.result(table.put(
                    rKeyBuf,
                    lKeyBuf,
                    valBuf,
                    option))) {
                    assertNull(putResult.prevKv());
                }
            }
        }

        // put second key
        ByteBuf valBuf2 = Unpooled.wrappedBuffer("testing-value-2".getBytes(UTF_8));
        try (PutOptionBuilder<ByteBuf> optionBuilder = optionFactory.newPutOption().prevKv(true)) {
            try (PutOption<ByteBuf> option = optionBuilder.build()) {
                 try (PutResult<ByteBuf, ByteBuf> putResult = FutureUtils.result(table.put(
                    rKeyBuf,
                    lKeyBuf,
                    valBuf2,
                    option))) {
                     assertNotNull(putResult.prevKv());
                     KeyValue<ByteBuf, ByteBuf> prevKv = putResult.prevKv();
                     assertEquals("testing-key", new String(ByteBufUtil.getBytes(prevKv.key()), UTF_8));
                     assertEquals("testing-value", new String(ByteBufUtil.getBytes(prevKv.value()), UTF_8));
                 }
            }
        }

        // get key
        try (RangeOptionBuilder<ByteBuf> optionBuilder = optionFactory.newRangeOption()) {
            try (RangeOption<ByteBuf> option = optionBuilder.build()) {
                try (RangeResult<ByteBuf, ByteBuf> getResult = FutureUtils.result(table.get(
                    rKeyBuf,
                    lKeyBuf,
                    option
                ))) {
                    assertEquals(1, getResult.count());
                    assertEquals(1, getResult.kvs().size());
                    KeyValue<ByteBuf, ByteBuf> kv = getResult.kvs().get(0);
                    assertEquals("testing-key", new String(ByteBufUtil.getBytes(kv.key()), UTF_8));
                    assertEquals("testing-value-2", new String(ByteBufUtil.getBytes(kv.value()), UTF_8));
                }
            }
        }

        // delete key
        try (DeleteOptionBuilder<ByteBuf> optionBuilder = optionFactory.newDeleteOption().prevKv(true)) {
            try (DeleteOption<ByteBuf> option = optionBuilder.build()) {
                try (DeleteResult<ByteBuf, ByteBuf> deleteResult = FutureUtils.result(table.delete(
                    rKeyBuf,
                    lKeyBuf,
                    option))) {
                    assertEquals(1, deleteResult.numDeleted());
                    assertEquals(1, deleteResult.prevKvs().size());
                    KeyValue<ByteBuf, ByteBuf> kv = deleteResult.prevKvs().get(0);
                    assertEquals("testing-key", new String(ByteBufUtil.getBytes(kv.key()), UTF_8));
                    assertEquals("testing-value-2", new String(ByteBufUtil.getBytes(kv.value()), UTF_8));
                }
            }
        }

        // write a range of key
        int numKvs = 100;
        rKeyBuf = Unpooled.wrappedBuffer("test-key".getBytes(UTF_8));
        try (PutOptionBuilder<ByteBuf> optionBuilder = optionFactory.newPutOption().prevKv(false)) {
            try (PutOption<ByteBuf> option = optionBuilder.build()) {
                for (int i = 0; i < numKvs; i++) {
                    lKeyBuf = getLKey(i);
                    valBuf = getValue(i);
                    FutureUtils.result(table.put(
                        rKeyBuf,
                        lKeyBuf,
                        valBuf,
                        option));
                }
            }
        }

        // get ranges
        ByteBuf lStartKey = getLKey(20);
        ByteBuf lEndKey = getLKey(50);
        try (RangeOptionBuilder<ByteBuf> optionBuilder = optionFactory.newRangeOption().endKey(lEndKey)) {
            try (RangeOption<ByteBuf> option = optionBuilder.build()) {
                 try (RangeResult<ByteBuf, ByteBuf> rangeResult = FutureUtils.result(table.get(
                     rKeyBuf,
                     lStartKey,
                     option))) {
                     assertEquals(31, rangeResult.kvs().size());
                     assertEquals(31, rangeResult.count());
                     int i = 20;
                     for (KeyValue<ByteBuf, ByteBuf> kvPair : rangeResult.kvs()) {
                         assertEquals(getLKey(i), kvPair.key());
                         assertEquals(getValue(i), kvPair.value());
                         ++i;
                     }
                     assertEquals(51, i);
                 }
            }
        }

        // delete range
        try (DeleteOptionBuilder<ByteBuf> optionBuilder = optionFactory.newDeleteOption()
             .prevKv(true)
             .endKey(lEndKey)) {
            try (DeleteOption<ByteBuf> option = optionBuilder.build()) {
                try (DeleteResult<ByteBuf, ByteBuf> deleteRangeResult = FutureUtils.result(table.delete(
                    rKeyBuf,
                    lStartKey,
                    option))) {
                    assertEquals(31, deleteRangeResult.numDeleted());
                    assertEquals(31, deleteRangeResult.prevKvs().size());
                    int i = 20;
                    for (KeyValue<ByteBuf, ByteBuf> kvPair : deleteRangeResult.prevKvs()) {
                        assertEquals(getLKey(i), kvPair.key());
                        assertEquals(getValue(i), kvPair.value());
                        ++i;
                    }
                    assertEquals(51, i);

                }
            }
        }

        // test txn
        byte[] lTxnKey = "txn-key".getBytes(UTF_8);
        ByteBuf lTxnKeyBuf = Unpooled.wrappedBuffer(lTxnKey);
        byte[] txnValue = "txn-value".getBytes(UTF_8);
        ByteBuf txnValueBuf = Unpooled.wrappedBuffer(txnValue);
        Txn<ByteBuf, ByteBuf> txn = table.txn(lTxnKeyBuf);

        CompletableFuture<TxnResult<ByteBuf, ByteBuf>> commitFuture = txn
            .If(
                table.opFactory().compareValue(CompareResult.EQUAL, lTxnKeyBuf, Unpooled.wrappedBuffer(new byte[0]))
            )
            .Then(
                table.opFactory().newPut(
                    lTxnKeyBuf, txnValueBuf, table.opFactory().optionFactory().newPutOption().build()))
            .commit();
        try (TxnResult<ByteBuf, ByteBuf> txnResult = FutureUtils.result(commitFuture)) {
            assertTrue(txnResult.isSuccess());
            assertEquals(1, txnResult.results().size());
            Result<ByteBuf, ByteBuf> opResult = txnResult.results().get(0);
            assertEquals(OpType.PUT, opResult.type());
        }

        // get key
        try (RangeOptionBuilder<ByteBuf> optionBuilder = optionFactory.newRangeOption()) {
            try (RangeOption<ByteBuf> option = optionBuilder.build()) {
                try (RangeResult<ByteBuf, ByteBuf> getResult = FutureUtils.result(table.get(
                    lTxnKeyBuf,
                    lTxnKeyBuf,
                    option
                ))) {
                    assertEquals(1, getResult.count());
                    assertEquals(1, getResult.kvs().size());
                    KeyValue<ByteBuf, ByteBuf> kv = getResult.kvs().get(0);
                    assertEquals("txn-key", new String(ByteBufUtil.getBytes(kv.key()), UTF_8));
                    assertEquals("txn-value", new String(ByteBufUtil.getBytes(kv.value()), UTF_8));
                }
            }
        }

        txn = table.txn(lTxnKeyBuf);
        // txn failure
        commitFuture = txn
            .If(
                table.opFactory().compareValue(CompareResult.EQUAL, lTxnKeyBuf, Unpooled.wrappedBuffer(new byte[0]))
            )
            .Then(
                table.opFactory().newPut(
                    lTxnKeyBuf, valBuf, table.opFactory().optionFactory().newPutOption().build()))
            .commit();
        try (TxnResult<ByteBuf, ByteBuf> txnResult = FutureUtils.result(commitFuture)) {
            assertFalse(txnResult.isSuccess());
            assertEquals(0, txnResult.results().size());
        }

    }
}
