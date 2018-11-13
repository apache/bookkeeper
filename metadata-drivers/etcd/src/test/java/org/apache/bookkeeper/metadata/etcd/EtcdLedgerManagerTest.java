/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.metadata.etcd;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.client.LedgerMetadataBuilder;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRange;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.metadata.etcd.helpers.ValueStream;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallbackFuture;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration test {@link EtcdLedgerManager}.
 */
@Slf4j
public class EtcdLedgerManagerTest extends EtcdTestBase {

    private String scope;
    private EtcdLedgerManager lm;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.scope = RandomStringUtils.randomAlphabetic(8);
        this.lm = new EtcdLedgerManager(etcdClient, scope);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (null != lm) {
            lm.close();
        }
        super.tearDown();
    }

    @Test
    public void testLedgerCRUD() throws Exception {
        long ledgerId = System.currentTimeMillis();
        LedgerMetadata metadata = new LedgerMetadata(
            3, 3, 2,
            DigestType.CRC32C,
            "test-password".getBytes(UTF_8)
        );

        // ledger doesn't exist: read

        GenericCallbackFuture<Versioned<LedgerMetadata>> readFuture = new GenericCallbackFuture<>();
        lm.readLedgerMetadata(ledgerId, readFuture);
        try {
            result(readFuture);
            fail("Should fail on reading ledger metadata if the ledger doesn't exist");
        } catch (BKException bke) {
            assertEquals(Code.NoSuchLedgerExistsException, bke.getCode());
        }

        // ledger doesn't exist : delete

        GenericCallbackFuture<Void> deleteFuture = new GenericCallbackFuture<>();
        lm.removeLedgerMetadata(ledgerId, new LongVersion(999L), deleteFuture);
        try {
            result(deleteFuture);
            fail("Should fail on deleting ledger metadata if the ledger doesn't exist");
        } catch (BKException bke) {
            assertEquals(Code.NoSuchLedgerExistsException, bke.getCode());
        }

        // ledger doesn't exist : write

        GenericCallbackFuture<Versioned<LedgerMetadata>> writeFuture = new GenericCallbackFuture<>();
        lm.writeLedgerMetadata(ledgerId, metadata, new LongVersion(999L), writeFuture);
        try {
            result(deleteFuture);
            fail("Should fail on updating ledger metadata if the ledger doesn't exist");
        } catch (BKException bke) {
            assertEquals(Code.NoSuchLedgerExistsException, bke.getCode());
        }

        // ledger doesn't exist : create

        GenericCallbackFuture<Versioned<LedgerMetadata>> createFuture = new GenericCallbackFuture<>();
        lm.createLedgerMetadata(ledgerId, metadata, createFuture);
        Versioned<LedgerMetadata> writtenMetadata = result(createFuture);
        assertSame(metadata, writtenMetadata.getValue());
        Version version = writtenMetadata.getVersion();
        assertNotNull(version);
        assertTrue(version instanceof LongVersion);
        assertTrue(((LongVersion) version).getLongVersion() > 0L);

        // ledger exists : create

        // attempt to create the ledger again will result in exception `LedgerExistsException`
        createFuture = new GenericCallbackFuture<>();
        try {
            lm.createLedgerMetadata(ledgerId, metadata, createFuture);
            result(createFuture);
            fail("Should fail on creating ledger metadata if the ledger already exists");
        } catch (BKException bke) {
            assertEquals(Code.LedgerExistException, bke.getCode());
        }

        // ledger exists: get

        readFuture = new GenericCallbackFuture<>();
        lm.readLedgerMetadata(ledgerId, readFuture);
        Versioned<LedgerMetadata> readMetadata = result(readFuture);
        assertEquals(metadata, readMetadata.getValue());

        // ledger exists: update metadata with wrong version
        writeFuture = new GenericCallbackFuture<>();
        lm.writeLedgerMetadata(ledgerId, readMetadata.getValue(), new LongVersion(Long.MAX_VALUE), writeFuture);
        try {
            result(writeFuture);
            fail("Should fail to write metadata using a wrong version");
        } catch (BKException bke) {
            assertEquals(Code.MetadataVersionException, bke.getCode());
        }
        readFuture = new GenericCallbackFuture<>();
        lm.readLedgerMetadata(ledgerId, readFuture);
        readMetadata = result(readFuture);
        assertEquals(metadata, readMetadata.getValue());

        // ledger exists: delete metadata with wrong version

        deleteFuture = new GenericCallbackFuture<>();
        lm.removeLedgerMetadata(ledgerId, new LongVersion(Long.MAX_VALUE), deleteFuture);
        try {
            result(deleteFuture);
            fail("Should fail to delete metadata using a wrong version");
        } catch (BKException bke) {
            assertEquals(Code.MetadataVersionException, bke.getCode());
        }
        readFuture = new GenericCallbackFuture<>();
        lm.readLedgerMetadata(ledgerId, readFuture);
        readMetadata = result(readFuture);
        assertEquals(metadata, readMetadata.getValue());

        // ledger exists: update metadata with the right version

        LongVersion curVersion = (LongVersion) readMetadata.getVersion();
        writeFuture = new GenericCallbackFuture<>();
        lm.writeLedgerMetadata(ledgerId, readMetadata.getValue(), curVersion, writeFuture);
        writtenMetadata = result(writeFuture);
        LongVersion newVersion = (LongVersion) writtenMetadata.getVersion();
        assertTrue(curVersion.getLongVersion() < newVersion.getLongVersion());
        readFuture = new GenericCallbackFuture<>();
        lm.readLedgerMetadata(ledgerId, readFuture);
        readMetadata = result(readFuture);
        assertEquals(writtenMetadata, readMetadata);

        // ledger exists: delete metadata with the right version

        deleteFuture = new GenericCallbackFuture<>();
        lm.removeLedgerMetadata(ledgerId, newVersion, deleteFuture);
        result(deleteFuture);
        readFuture = new GenericCallbackFuture<>();
        try {
            lm.readLedgerMetadata(ledgerId, readFuture);
            result(readFuture);
            fail("Should fail to read ledger if it is deleted");
        } catch (BKException bke) {
            assertEquals(Code.NoSuchLedgerExistsException, bke.getCode());
        }

    }

    @Test
    public void testProcessLedgers() throws Exception {
        final int numLedgers = 100;
        createNumLedgers(numLedgers);

        final CountDownLatch processLatch = new CountDownLatch(numLedgers);
        final CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        lm.asyncProcessLedgers(
            (l, cb) -> processLatch.countDown(),
            (rc, path, ctx) -> {
                if (Code.OK == rc) {
                    FutureUtils.complete(doneFuture, null);
                } else {
                    FutureUtils.completeExceptionally(doneFuture, BKException.create(rc));
                }
            },
            null,
            Code.OK,
            Code.MetaStoreException);

        result(doneFuture);
        processLatch.await();
    }

    @Test
    public void testLedgerRangeIterator() throws Exception {
        final int numLedgers = 100;
        createNumLedgers(numLedgers);

        long nextLedgerId = 0L;
        LedgerRangeIterator iter = lm.getLedgerRanges();
        while (iter.hasNext()) {
            LedgerRange lr = iter.next();
            for (Long lid : lr.getLedgers()) {
                assertEquals(nextLedgerId, lid.longValue());
                ++nextLedgerId;
            }
        }
        assertEquals((long) numLedgers, nextLedgerId);
    }

    private void createNumLedgers(int numLedgers) throws Exception {
        List<CompletableFuture<Versioned<LedgerMetadata>>> createFutures = new ArrayList<>(numLedgers);
        for (int i = 0; i < numLedgers; i++) {
            GenericCallbackFuture<Versioned<LedgerMetadata>> createFuture = new GenericCallbackFuture<>();
            createFutures.add(createFuture);
            LedgerMetadata metadata = new LedgerMetadata(
                3, 3, 2,
                DigestType.CRC32C,
                "test-password".getBytes(UTF_8)
            );
            lm.createLedgerMetadata(i, metadata, createFuture);
        }
        FutureUtils.result(FutureUtils.collect(createFutures));
    }

    @Test
    public void testRegisterLedgerMetadataListener() throws Exception {
        long ledgerId = System.currentTimeMillis();

        // create a ledger metadata
        LedgerMetadata metadata = new LedgerMetadata(
            3, 3, 2,
            DigestType.CRC32C,
            "test-password".getBytes(UTF_8)
        );
        metadata.addEnsemble(0L, createNumBookies(3));
        GenericCallbackFuture<Versioned<LedgerMetadata>> createFuture = new GenericCallbackFuture<>();
        lm.createLedgerMetadata(ledgerId, metadata, createFuture);
        result(createFuture);
        Versioned<LedgerMetadata> readMetadata = readLedgerMetadata(ledgerId);
        log.info("Create ledger metadata : {}", readMetadata.getValue());

        // register first listener

        LinkedBlockingQueue<Versioned<LedgerMetadata>> metadataQueue1 = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener1 = (lid, m) -> {
            log.info("[listener1] Received ledger {} metadata : {}", lid, m);
            metadataQueue1.add(m);
        };
        log.info("Registered first listener for ledger {}", ledgerId);
        lm.registerLedgerMetadataListener(ledgerId, listener1);
        // we should receive a metadata notification when a ledger is created
        Versioned<LedgerMetadata> notifiedMetadata = metadataQueue1.take();
        assertEquals(readMetadata, notifiedMetadata);
        ValueStream<LedgerMetadata> lms = lm.getLedgerMetadataStream(ledgerId);
        assertNotNull(lms.waitUntilWatched());
        assertNotNull(result(lms.waitUntilWatched()));

        // register second listener

        LinkedBlockingQueue<Versioned<LedgerMetadata>> metadataQueue2 = new LinkedBlockingQueue<>();
        LedgerMetadataListener listener2 = (lid, m) -> {
            log.info("[listener2] Received ledger {} metadata : {}", lid, m);
            metadataQueue2.add(m);
        };
        log.info("Registered second listener for ledger {}", ledgerId);
        lm.registerLedgerMetadataListener(ledgerId, listener2);
        Versioned<LedgerMetadata> notifiedMetadata2 = metadataQueue2.take();
        assertEquals(readMetadata, notifiedMetadata2);
        assertNotNull(lm.getLedgerMetadataStream(ledgerId));

        // update the metadata
        writeLedgerMetadata(ledgerId,
                            LedgerMetadataBuilder.from(metadata).newEnsembleEntry(10L, createNumBookies(3)).build(),
                            notifiedMetadata.getVersion());
        readMetadata = readLedgerMetadata(ledgerId);
        assertEquals(readMetadata, metadataQueue1.take());
        assertEquals(readMetadata, metadataQueue2.take());
        lms = lm.getLedgerMetadataStream(ledgerId);
        assertNotNull(lms);
        assertEquals(2, lms.getNumConsumers());

        // remove listener2
        lm.unregisterLedgerMetadataListener(ledgerId, listener2);
        lms = lm.getLedgerMetadataStream(ledgerId);
        assertNotNull(lms);
        assertEquals(1, lms.getNumConsumers());

        // update the metadata again
        writeLedgerMetadata(ledgerId,
                            LedgerMetadataBuilder.from(metadata).newEnsembleEntry(20L, createNumBookies(3)).build(),
                            readMetadata.getVersion());
        readMetadata = readLedgerMetadata(ledgerId);
        assertEquals(readMetadata, metadataQueue1.take());
        assertNull(metadataQueue2.poll());

        // remove listener1
        lm.unregisterLedgerMetadataListener(ledgerId, listener1);
        // the value stream will be removed
        while (lm.getLedgerMetadataStream(ledgerId) != null) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        assertEquals(0, lms.getNumConsumers());

        // update the metadata again
        writeLedgerMetadata(ledgerId,
                            LedgerMetadataBuilder.from(metadata).newEnsembleEntry(30L, createNumBookies(3)).build(),
                            readMetadata.getVersion());
        readMetadata = readLedgerMetadata(ledgerId);
        assertNull(metadataQueue1.poll());
        assertNull(metadataQueue2.poll());

        log.info("Registered first listener for ledger {} again", ledgerId);
        lm.registerLedgerMetadataListener(ledgerId, listener1);
        notifiedMetadata = metadataQueue1.take();
        assertEquals(readMetadata, notifiedMetadata);
        lms = lm.getLedgerMetadataStream(ledgerId);
        assertNotNull(lms);
        assertEquals(1, lms.getNumConsumers());

        // delete the ledger
        removeLedgerMetadata(ledgerId, readMetadata.getVersion());
        // the listener will eventually be removed
        while (lm.getLedgerMetadataStream(ledgerId) != null) {
            TimeUnit.MILLISECONDS.sleep(100);
        }
        assertEquals(1, lms.getNumConsumers());
        assertNull(metadataQueue1.poll());
        assertNull(metadataQueue2.poll());
    }

    Versioned<LedgerMetadata> readLedgerMetadata(long lid) throws Exception {
        GenericCallbackFuture<Versioned<LedgerMetadata>> readFuture = new GenericCallbackFuture<>();
        lm.readLedgerMetadata(lid, readFuture);
        return result(readFuture);
    }

    void writeLedgerMetadata(long lid, LedgerMetadata metadata, Version version) throws Exception {
        GenericCallbackFuture<Versioned<LedgerMetadata>> writeFuture = new GenericCallbackFuture<>();
        lm.writeLedgerMetadata(lid, metadata, version, writeFuture);
        result(writeFuture);
    }

    void removeLedgerMetadata(long lid, Version version) throws Exception {
        GenericCallbackFuture<Void> deleteFuture = new GenericCallbackFuture<>();
        lm.removeLedgerMetadata(lid, version, deleteFuture);
        result(deleteFuture);
    }

    static List<BookieSocketAddress> createNumBookies(int numBookies) {
        return IntStream.range(0, numBookies)
            .mapToObj(idx -> new BookieSocketAddress("127.0.0.1", 3181 + idx))
            .collect(Collectors.toList());
    }
}
