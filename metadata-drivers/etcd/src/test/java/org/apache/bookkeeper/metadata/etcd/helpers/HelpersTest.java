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

package org.apache.bookkeeper.metadata.etcd.helpers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.etcd.jetcd.ByteSequence;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test helpers.
 */
@Slf4j
public class HelpersTest extends EtcdTestBase {

    private static final Function<ByteSequence, String> BYTE_SEQUENCE_STRING_FUNCTION =
            bs -> bs.toString(UTF_8);

    private static String getKey(String scope, int i) {
        return String.format("%s-key-%010d", scope, i);
    }

    private String scope;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        scope = RandomStringUtils.randomAlphabetic(8);
    }

    @Test
    public void testEmptyKeyStream() throws Exception {
        KeyStream<String> ks = new KeyStream<>(
            etcdClient.getKVClient(),
            ByteSequence.from(getKey(scope, 0), UTF_8),
            ByteSequence.from(getKey(scope, 100), UTF_8),
            BYTE_SEQUENCE_STRING_FUNCTION
        );
        List<String> values = result(ks.readNext());
        assertTrue(values.isEmpty());

        // read the values again
        values = result(ks.readNext());
        assertTrue(values.isEmpty());
    }

    @Test
    public void testKeyStreamBatch1() throws Exception {
        testKeyStream(20, 1);
    }

    @Test
    public void testKeyStreamBatch2() throws Exception {
        testKeyStream(20, 2);
    }

    @Test
    public void testKeyStreamBatch7() throws Exception {
        testKeyStream(20, 7);
    }

    @Test
    public void testKeyStreamBatch10() throws Exception {
        testKeyStream(20, 10);
    }

    @Test
    public void testKeyStreamBatch20() throws Exception {
        testKeyStream(20, 20);
    }

    @Test
    public void testKeyStreamBatch40() throws Exception {
        testKeyStream(20, 40);
    }

    @Test
    public void testKeyStreamBatchUnlimited() throws Exception {
        testKeyStream(20, 0);
    }

    private void testKeyStream(int numKeys, int batchSize) throws Exception {
        for (int i = 0; i < numKeys; i++) {
            String key = getKey(scope, i);
            ByteSequence keyBs = ByteSequence.from(key.getBytes(UTF_8));
            result(etcdClient.getKVClient().put(keyBs, keyBs));
        }

        KeyStream<Integer> ks = openKeyStream(batchSize);
        AtomicInteger numReceived = new AtomicInteger(0);
        while (true) {
            List<Integer> values = result(ks.readNext());
            log.info("Received values : {}", values);
            if (values.isEmpty()) {
                break;
            }
            for (int value : values) {
                assertEquals(numReceived.getAndIncrement(), value);
            }
        }
        assertEquals(numKeys, numReceived.get());
    }

    private void testKeyIterator(int numKeys, int batchSize) throws Exception {
        for (int i = 0; i < numKeys; i++) {
            String key = getKey(scope, i);
            ByteSequence keyBs = ByteSequence.from(key, UTF_8);
            result(etcdClient.getKVClient().put(keyBs, keyBs));
        }

        KeyStream<Integer> ks = openKeyStream(batchSize);
        KeyIterator<Integer> ki = new KeyIterator<>(ks);

        AtomicInteger numReceived = new AtomicInteger(0);
        while (ki.hasNext()) {
            List<Integer> values = ki.next();
            log.info("Received values : {}", values);
            if (values.isEmpty()) {
                break;
            }
            for (int value : values) {
                assertEquals(numReceived.getAndIncrement(), value);
            }
        }
        assertEquals(numKeys, numReceived.get());
    }

    @Test
    public void testKeyIteratorBatch1() throws Exception {
        testKeyIterator(20, 1);
    }

    @Test
    public void testKeyIteratorBatch2() throws Exception {
        testKeyIterator(20, 2);
    }

    @Test
    public void testKeyIteratorBatch7() throws Exception {
        testKeyIterator(20, 7);
    }

    @Test
    public void testKeyIteratorBatch10() throws Exception {
        testKeyIterator(20, 10);
    }

    @Test
    public void testKeyIteratorBatch20() throws Exception {
        testKeyIterator(20, 20);
    }

    @Test
    public void testKeyIteratorBatch40() throws Exception {
        testKeyIterator(20, 40);
    }

    @Test
    public void testKeyIteratorBatchUnlimited() throws Exception {
        testKeyIterator(20, 0);
    }

    private KeyStream<Integer> openKeyStream(int batchSize) {
        KeyStream<Integer> ks = new KeyStream<>(
            etcdClient.getKVClient(),
            ByteSequence.from(getKey(scope, 0).getBytes(UTF_8)),
            ByteSequence.from(getKey(scope, Integer.MAX_VALUE).getBytes(UTF_8)),
                bs -> {
                String[] keyParts = StringUtils.split(bs.toString(UTF_8), '-');
                try {
                    return Integer.parseInt(keyParts[2]);
                } catch (NumberFormatException nfe) {
                    log.error("Failed to parse key string '{}' : ",
                        bs.toString(UTF_8), nfe);
                    return -0xabcd;
                }
            },
            batchSize
        );
        return ks;
    }

}
