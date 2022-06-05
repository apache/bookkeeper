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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.support.Observers;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.compress.utils.Sets;
import org.junit.Test;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

/**
 * Integration test {@link KeySetReader}.
 */
@Slf4j
public class KeySetReaderTest extends EtcdTestBase {

    private static final Function<ByteSequence, String> BYTE_SEQUENCE_STRING_FUNCTION =
            bs -> bs.toString(StandardCharsets.UTF_8);

    @Test
    public void testReadSingleKey() throws Exception {
        String key = RandomStringUtils.randomAlphabetic(16);
        ByteSequence keyBs = ByteSequence.from(key, StandardCharsets.UTF_8);
        try (KeySetReader<String> ksReader = new KeySetReader<>(
            etcdClient,
            BYTE_SEQUENCE_STRING_FUNCTION,
            keyBs,
            null
        )) {
            // key not exists
            Versioned<Set<String>> versionedKeys = FutureUtils.result(ksReader.read());
            assertTrue(
                "VersionedKeys : " + versionedKeys,
                ((LongVersion) versionedKeys.getVersion()).getLongVersion() > 0L);
            assertEquals(0, versionedKeys.getValue().size());
            assertFalse(ksReader.isWatcherSet());

            // keys should be cached
            assertEquals(versionedKeys, ksReader.getLocalValue());

            // update a value
            String value = RandomStringUtils.randomAlphabetic(32);
            ByteSequence valueBs = ByteSequence.from(value, StandardCharsets.UTF_8);
            FutureUtils.result(etcdClient.getKVClient().put(keyBs, valueBs));

            // update the value should not change local value
            assertEquals(versionedKeys, ksReader.getLocalValue());

            // read the key again
            Versioned<Set<String>> newVersionedKey = FutureUtils.result(ksReader.read());
            assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(1, newVersionedKey.getValue().size());
            assertEquals(Sets.newHashSet(key), newVersionedKey.getValue());

            // local value should be changed
            assertEquals(newVersionedKey, ksReader.getLocalValue());
        }
    }

    @Test
    public void testWatchSingleKey() throws Exception {
        String key = RandomStringUtils.randomAlphabetic(16);
        ByteSequence keyBs = ByteSequence.from(key, StandardCharsets.UTF_8);
        KeySetReader<String> ksReader = null;
        try {
            ksReader = new KeySetReader<>(
                etcdClient,
                BYTE_SEQUENCE_STRING_FUNCTION,
                keyBs,
                null
            );
            LinkedBlockingQueue<Versioned<Set<String>>> notifications = new LinkedBlockingQueue<>();
            Consumer<Versioned<Set<String>>> keyConsumer = consumeVersionedKeySet(notifications);

            // key not exists
            Versioned<Set<String>> versionedKeys = FutureUtils.result(ksReader.readAndWatch(keyConsumer));
            assertTrue(
                "VersionedKeys : " + versionedKeys,
                ((LongVersion) versionedKeys.getVersion()).getLongVersion() > 0L);
            assertEquals(0, versionedKeys.getValue().size());
            assertTrue(ksReader.isWatcherSet());

            // keys should be cached
            assertEquals(versionedKeys, ksReader.getLocalValue());
            Versioned<Set<String>> newVersionedKey = notifications.take();
            assertEquals(Occurred.CONCURRENTLY, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(versionedKeys, newVersionedKey);
            versionedKeys = newVersionedKey;

            // update a value
            String value = RandomStringUtils.randomAlphabetic(32);
            ByteSequence valueBs = ByteSequence.from(value, StandardCharsets.UTF_8);
            FutureUtils.result(etcdClient.getKVClient().put(keyBs, valueBs));

            // we should get notified with updated key set
            newVersionedKey = notifications.take();
            assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(1, newVersionedKey.getValue().size());
            assertEquals(Sets.newHashSet(key), newVersionedKey.getValue());

            // local value should be changed
            assertEquals(newVersionedKey, ksReader.getLocalValue());
            versionedKeys = newVersionedKey;

            // delete the key
            FutureUtils.result(etcdClient.getKVClient().delete(keyBs));
            newVersionedKey = notifications.take();
            assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(0, newVersionedKey.getValue().size());

            // local value should be changed
            assertEquals(newVersionedKey, ksReader.getLocalValue());
        } finally {
            if (null != ksReader) {
                ksReader.close();
            }
        }
        assertNotNull(ksReader);
        assertFalse(ksReader.isWatcherSet());
    }

    @Test
    public void testWatchSingleKeyWithTTL() throws Exception {
        String key = RandomStringUtils.randomAlphabetic(16);
        ByteSequence keyBs = ByteSequence.from(key, StandardCharsets.UTF_8);
        KeySetReader<String> ksReader = null;
        try {
            ksReader = new KeySetReader<>(
                etcdClient,
                BYTE_SEQUENCE_STRING_FUNCTION,
                keyBs,
                null
            );
            LinkedBlockingQueue<Versioned<Set<String>>> notifications = new LinkedBlockingQueue<>();
            Consumer<Versioned<Set<String>>> keyConsumer = consumeVersionedKeySet(notifications);

            // key not exists
            Versioned<Set<String>> versionedKeys = FutureUtils.result(ksReader.readAndWatch(keyConsumer));
            assertTrue(
                "VersionedKeys : " + versionedKeys,
                ((LongVersion) versionedKeys.getVersion()).getLongVersion() > 0L);
            assertEquals(0, versionedKeys.getValue().size());
            assertTrue(ksReader.isWatcherSet());

            // keys should be cached
            assertEquals(versionedKeys, ksReader.getLocalValue());
            // no watch event should be issued
            Versioned<Set<String>> newVersionedKey = notifications.take();
            assertEquals(Occurred.CONCURRENTLY, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(versionedKeys, newVersionedKey);
            versionedKeys = newVersionedKey;

            // create a key with ttl
            long leaseId = FutureUtils.result(etcdClient.getLeaseClient().grant(1)).getID();
            String value = RandomStringUtils.randomAlphabetic(32);
            ByteSequence valueBs = ByteSequence.from(value, StandardCharsets.UTF_8);
            FutureUtils.result(etcdClient.getKVClient()
                .put(keyBs, valueBs, PutOption.newBuilder().withLeaseId(leaseId).build()));

            // we should get notified with updated key set
            newVersionedKey = notifications.take();
            assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(1, newVersionedKey.getValue().size());
            assertEquals(Sets.newHashSet(key), newVersionedKey.getValue());

            // local value should be changed
            assertEquals(newVersionedKey, ksReader.getLocalValue());
            versionedKeys = newVersionedKey;

            // the key will be deleted after TTL
            newVersionedKey = notifications.take();
            assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(0, newVersionedKey.getValue().size());

            // local value should be changed
            assertEquals(newVersionedKey, ksReader.getLocalValue());
        } finally {
            if (null != ksReader) {
                ksReader.close();
            }
        }
        assertNotNull(ksReader);
        assertFalse(ksReader.isWatcherSet());
    }

    @Test
    public void testReadKeySet() throws Exception {
        String prefix = RandomStringUtils.randomAlphabetic(16);
        ByteSequence beginKeyBs = ByteSequence.from(prefix + "-000", StandardCharsets.UTF_8);
        ByteSequence endKeyBs = ByteSequence.from(prefix + "-999", StandardCharsets.UTF_8);
        try (KeySetReader<String> ksReader = new KeySetReader<>(
            etcdClient,
            BYTE_SEQUENCE_STRING_FUNCTION,
            beginKeyBs,
            endKeyBs
        )) {
            // key not exists
            Versioned<Set<String>> versionedKeys = FutureUtils.result(ksReader.read());
            assertTrue(
                "VersionedKeys : " + versionedKeys,
                ((LongVersion) versionedKeys.getVersion()).getLongVersion() > 0L);
            assertEquals(0, versionedKeys.getValue().size());
            assertFalse(ksReader.isWatcherSet());

            // keys should be cached
            assertEquals(versionedKeys, ksReader.getLocalValue());

            Set<String> expectedKeySet = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                // update a value
                String key = String.format("%s-%03d", prefix, i);
                String value = RandomStringUtils.randomAlphabetic(32);
                ByteSequence keyBs = ByteSequence.from(key, StandardCharsets.UTF_8);
                ByteSequence valueBs = ByteSequence.from(value, StandardCharsets.UTF_8);
                expectedKeySet.add(key);
                FutureUtils.result(etcdClient.getKVClient().put(keyBs, valueBs));

                // update the value should not change local value
                assertEquals(versionedKeys, ksReader.getLocalValue());

                // read the key again
                Versioned<Set<String>> newVersionedKey = FutureUtils.result(ksReader.read());
                assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
                assertEquals(expectedKeySet, newVersionedKey.getValue());

                // local value should be changed
                assertEquals(newVersionedKey, ksReader.getLocalValue());
                versionedKeys = newVersionedKey;
            }
        }
    }

    @Test
    public void testWatchKeySet() throws Exception {
        String prefix = RandomStringUtils.randomAlphabetic(16);
        ByteSequence beginKeyBs = ByteSequence.from(prefix + "-000", StandardCharsets.UTF_8);
        ByteSequence endKeyBs = ByteSequence.from(prefix + "-999", StandardCharsets.UTF_8);
        KeySetReader<String> ksReader = null;
        try {
            ksReader = new KeySetReader<>(
                etcdClient,
                BYTE_SEQUENCE_STRING_FUNCTION,
                beginKeyBs,
                endKeyBs
            );
            LinkedBlockingQueue<Versioned<Set<String>>> notifications = new LinkedBlockingQueue<>();
            Consumer<Versioned<Set<String>>> keyConsumer = consumeVersionedKeySet(notifications);

            // key not exists
            Versioned<Set<String>> versionedKeys = FutureUtils.result(ksReader.readAndWatch(keyConsumer));
            assertTrue(
                "VersionedKeys : " + versionedKeys,
                ((LongVersion) versionedKeys.getVersion()).getLongVersion() > 0L);
            assertEquals(0, versionedKeys.getValue().size());
            assertTrue(ksReader.isWatcherSet());

            // keys should be cached
            assertEquals(versionedKeys, ksReader.getLocalValue());
            Versioned<Set<String>> newVersionedKey = notifications.take();
            assertEquals(Occurred.CONCURRENTLY, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(versionedKeys, newVersionedKey);
            versionedKeys = newVersionedKey;

            Set<String> expectedKeySet = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                // update a value
                String key = String.format("%s-%03d", prefix, i);
                String value = RandomStringUtils.randomAlphabetic(32);
                ByteSequence keyBs = ByteSequence.from(key, StandardCharsets.UTF_8);
                ByteSequence valueBs = ByteSequence.from(value, StandardCharsets.UTF_8);
                expectedKeySet.add(key);
                FutureUtils.result(etcdClient.getKVClient().put(keyBs, valueBs));

                // we should get notified with updated key set
                newVersionedKey = notifications.take();
                assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
                assertEquals(expectedKeySet, newVersionedKey.getValue());

                // local value should be changed
                assertEquals(newVersionedKey, ksReader.getLocalValue());
                versionedKeys = newVersionedKey;
            }

            for (int i = 0; i < 20; i++) {
                // delete the key
                String key = String.format("%s-%03d", prefix, i);
                ByteSequence keyBs = ByteSequence.from(key, StandardCharsets.UTF_8);
                expectedKeySet.remove(key);
                FutureUtils.result(etcdClient.getKVClient().delete(keyBs));

                // we should get notified with updated key set
                newVersionedKey = notifications.take();
                assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
                assertEquals(expectedKeySet, newVersionedKey.getValue());

                // local value should be changed
                assertEquals(newVersionedKey, ksReader.getLocalValue());
                versionedKeys = newVersionedKey;
            }
        } finally {
            if (null != ksReader) {
                ksReader.close();
            }
        }
        assertNotNull(ksReader);
        assertFalse(ksReader.isWatcherSet());
    }

    @Test
    public void testWatchKeySetWithTTL() throws Exception {
        String prefix = RandomStringUtils.randomAlphabetic(16);
        ByteSequence beginKeyBs = ByteSequence.from(prefix + "-000", StandardCharsets.UTF_8);
        ByteSequence endKeyBs = ByteSequence.from(prefix + "-999", StandardCharsets.UTF_8);
        KeySetReader<String> ksReader = null;
        try {
            ksReader = new KeySetReader<>(
                etcdClient,
                BYTE_SEQUENCE_STRING_FUNCTION,
                beginKeyBs,
                endKeyBs
            );
            LinkedBlockingQueue<Versioned<Set<String>>> notifications = new LinkedBlockingQueue<>();
            Consumer<Versioned<Set<String>>> keyConsumer = consumeVersionedKeySet(notifications);

            // key not exists
            Versioned<Set<String>> versionedKeys = FutureUtils.result(ksReader.readAndWatch(keyConsumer));
            assertTrue(
                "VersionedKeys : " + versionedKeys,
                ((LongVersion) versionedKeys.getVersion()).getLongVersion() > 0L);
            assertEquals(0, versionedKeys.getValue().size());
            assertTrue(ksReader.isWatcherSet());

            // keys should be cached
            assertEquals(versionedKeys, ksReader.getLocalValue());
            // no watch event should be issued
            Versioned<Set<String>> newVersionedKey = notifications.take();
            assertEquals(Occurred.CONCURRENTLY, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertEquals(versionedKeys, newVersionedKey);
            versionedKeys = newVersionedKey;

            // create keys with ttl
            long leaseId = FutureUtils.result(etcdClient.getLeaseClient().grant(1)).getID();
            CloseableClient ka = etcdClient.getLeaseClient().keepAlive(leaseId, Observers.observer(response -> {
            }));

            Set<String> expectedKeySet = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                String key = String.format("%s-%03d", prefix, i);
                String value = RandomStringUtils.randomAlphabetic(32);
                ByteSequence keyBs = ByteSequence.from(key, StandardCharsets.UTF_8);
                ByteSequence valueBs = ByteSequence.from(value, StandardCharsets.UTF_8);
                expectedKeySet.add(key);
                FutureUtils.result(etcdClient.getKVClient()
                    .put(keyBs, valueBs, PutOption.newBuilder().withLeaseId(leaseId).build()));

                // we should get notified with updated key set
                newVersionedKey = notifications.take();
                assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
                assertEquals(expectedKeySet, newVersionedKey.getValue());

                // local value should be changed
                assertEquals(newVersionedKey, ksReader.getLocalValue());
                versionedKeys = newVersionedKey;
            }

            // stop keep alive all the keys should be expired.
            ka.close();

            // all the keys will be deleted after TTL in same batch.
            newVersionedKey = notifications.take();
            // local value should be changed
            assertEquals(newVersionedKey, ksReader.getLocalValue());
            assertEquals(Occurred.AFTER, newVersionedKey.getVersion().compare(versionedKeys.getVersion()));
            assertTrue(newVersionedKey.getValue().isEmpty());
        } finally {
            if (null != ksReader) {
                ksReader.close();
            }
        }
        assertNotNull(ksReader);
        assertFalse(ksReader.isWatcherSet());
    }
}
