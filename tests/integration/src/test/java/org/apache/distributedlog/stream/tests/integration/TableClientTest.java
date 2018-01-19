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
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.kv.KV;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.api.StorageClient;
import org.apache.distributedlog.api.kv.Table;
import org.apache.distributedlog.api.kv.options.DeleteOption;
import org.apache.distributedlog.api.kv.options.GetOption;
import org.apache.distributedlog.api.kv.options.PutOption;
import org.apache.distributedlog.api.kv.result.DeleteResult;
import org.apache.distributedlog.api.kv.result.GetResult;
import org.apache.distributedlog.api.kv.result.PutResult;
import org.apache.distributedlog.clients.StorageClientBuilder;
import org.apache.distributedlog.clients.admin.StorageAdminClient;
import org.apache.distributedlog.clients.config.StorageClientSettings;
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
        Table table = FutureUtils.result(storageClient.openTable(streamName));
        byte[] rKey = "routing-key".getBytes(UTF_8);
        byte[] lKey = "testing-key".getBytes(UTF_8);
        byte[] value = "testing-value".getBytes(UTF_8);

        // put first key
        ByteBuf rKeyBuf = Unpooled.wrappedBuffer(rKey);
        ByteBuf lKeyBuf = Unpooled.wrappedBuffer(lKey);
        ByteBuf valBuf = Unpooled.wrappedBuffer(value);
        PutResult putResult = FutureUtils.result(table.put(
            rKeyBuf,
            lKeyBuf,
            valBuf,
            PutOption.newBuilder()
                .prevKv(true)
                .build()));
        assertEquals("routing-key", new String(ByteBufUtil.getBytes(putResult.getPKey()), UTF_8));
        assertFalse(putResult.getPrevKv().isPresent());

        // put second key
        ByteBuf valBuf2 = Unpooled.wrappedBuffer("testing-value-2".getBytes(UTF_8));
        putResult = FutureUtils.result(table.put(
            rKeyBuf,
            lKeyBuf,
            valBuf2,
            PutOption.newBuilder()
                .prevKv(true)
                .build()));
        assertEquals("routing-key", new String(ByteBufUtil.getBytes(putResult.getPKey()), UTF_8));
        assertTrue(putResult.getPrevKv().isPresent());
        KV<ByteBuf, ByteBuf> prevKv = putResult.getPrevKv().get();
        assertEquals("testing-key", new String(ByteBufUtil.getBytes(prevKv.key()), UTF_8));
        assertEquals("testing-value", new String(ByteBufUtil.getBytes(prevKv.value()), UTF_8));

        // get key
        GetResult getResult = FutureUtils.result(table.get(
            rKeyBuf,
            lKeyBuf,
            GetOption.newBuilder().build()
        ));
        assertEquals("routing-key", new String(ByteBufUtil.getBytes(getResult.getPKey()), UTF_8));
        assertEquals(1, getResult.getCount());
        assertEquals(1, getResult.getKvs().size());
        KV<ByteBuf, ByteBuf> kv = getResult.getKvs().get(0);
        assertEquals("testing-key", new String(ByteBufUtil.getBytes(kv.key()), UTF_8));
        assertEquals("testing-value-2", new String(ByteBufUtil.getBytes(kv.value()), UTF_8));

        // delete key
        DeleteResult deleteResult = FutureUtils.result(table.delete(
            rKeyBuf,
            lKeyBuf,
            DeleteOption.newBuilder().prevKv(true).build()));
        assertEquals("routing-key", new String(ByteBufUtil.getBytes(deleteResult.getPKey()), UTF_8));
        assertEquals(1, deleteResult.getNumDeleted());
        assertEquals(1, deleteResult.getPrevKvs().size());
        kv = deleteResult.getPrevKvs().get(0);
        assertEquals("testing-key", new String(ByteBufUtil.getBytes(kv.key()), UTF_8));
        assertEquals("testing-value-2", new String(ByteBufUtil.getBytes(kv.value()), UTF_8));

        // write a range of key
        int numKvs = 100;
        rKeyBuf = Unpooled.wrappedBuffer("test-key".getBytes(UTF_8));
        for (int i = 0; i < numKvs; i++) {
            lKeyBuf = getLKey(i);
            valBuf = getValue(i);
            FutureUtils.result(table.put(
                rKeyBuf,
                lKeyBuf,
                valBuf,
                PutOption.newBuilder().prevKv(false).build()));
        }

        // get ranges
        ByteBuf lStartKey = getLKey(20);
        ByteBuf lEndKey = getLKey(50);
        GetResult rangeResult = FutureUtils.result(table.get(
            rKeyBuf,
            lStartKey,
            GetOption.newBuilder()
                .endKey(lEndKey)
                .build()
        ));
        assertEquals("test-key", new String(ByteBufUtil.getBytes(rangeResult.getPKey()), UTF_8));
        assertEquals(31, rangeResult.getCount());
        assertEquals(31, rangeResult.getKvs().size());
        int i = 20;
        for (KV<ByteBuf, ByteBuf> kvPair : rangeResult.getKvs()) {
            assertEquals(getLKey(i), kvPair.key());
            assertEquals(getValue(i), kvPair.value());
            ++i;
        }
        assertEquals(51, i);

        // delete range
        DeleteResult deleteRangeResult = FutureUtils.result(table.delete(
            rKeyBuf,
            lStartKey,
            DeleteOption.newBuilder()
                .endKey(lEndKey)
                .prevKv(true)
                .build()
        ));
        assertEquals("test-key", new String(ByteBufUtil.getBytes(deleteRangeResult.getPKey()), UTF_8));
        assertEquals(31, deleteRangeResult.getNumDeleted());
        assertEquals(31, deleteRangeResult.getPrevKvs().size());
        i = 20;
        for (KV<ByteBuf, ByteBuf> kvPair : deleteRangeResult.getPrevKvs()) {
            assertEquals(getLKey(i), kvPair.key());
            assertEquals(getValue(i), kvPair.value());
            ++i;
        }
        assertEquals(51, i);
    }
}
