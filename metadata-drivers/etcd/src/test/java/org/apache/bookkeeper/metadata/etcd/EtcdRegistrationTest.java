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

package org.apache.bookkeeper.metadata.etcd;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.coreos.jetcd.Client;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.metadata.etcd.testing.EtcdTestBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 * Test etcd based bookie registration.
 */
@Slf4j
public class EtcdRegistrationTest extends EtcdTestBase {

    static String newBookie(int i) {
        return "127.0.0.1:" + (3181 + i);
    }

    @Rule
    public final TestName runtime = new TestName();

    private String scope;
    private RegistrationClient regClient;

    protected static RegistrationListener newRegistrationListener(
        LinkedBlockingQueue<Versioned<Set<BookieSocketAddress>>> notifications) {
        return bookies -> {
            log.info("Received new bookies: {}", bookies);
            try {
                notifications.put(bookies);
            } catch (InterruptedException e) {
                log.error("Interrupted at enqueuing updated key set", e);
            }
        };
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        this.scope = RandomStringUtils.randomAlphabetic(16);
        this.regClient = new EtcdRegistrationClient(scope, etcdClient);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        this.regClient.close();
        super.tearDown();
    }

    interface MultiBookiesTester {

        void test(String scope, int numBookies, boolean readonly) throws Exception;

    }

    private static void runNumBookiesTest(final String scope,
                                          final int numBookies,
                                          final boolean readonly,
                                          MultiBookiesTester tester) throws Exception {

        final List<EtcdRegistrationManager> bookies = createNumBookies(readonly, numBookies, scope);
        try {
            tester.test(scope, numBookies, readonly);
        } finally {
            bookies.forEach(EtcdRegistrationManager::close);
        }

    }

    @Test
    public void testRegisterWritableBookies() throws Exception {
        testRegisterBookie(false);
    }

    @Test
    public void testRegisterReadonlyBookies() throws Exception {
        testRegisterBookie(true);
    }

    private void testRegisterBookie(boolean readonly) throws Exception {
        runNumBookiesTest(scope, 3, readonly, (scope, numBookies, ro) -> {
            Set<BookieSocketAddress> expectedBookies = Sets.newHashSet();
            for (int i = 0; i < numBookies; i++) {
                expectedBookies.add(new BookieSocketAddress(newBookie(i)));
            }
            Set<BookieSocketAddress> writableBookies = result(regClient.getWritableBookies()).getValue();
            Set<BookieSocketAddress> readonlyBookies = result(regClient.getReadOnlyBookies()).getValue();
            if (ro) {
                assertEquals(0, writableBookies.size());
                assertEquals(numBookies, readonlyBookies.size());
                assertEquals(expectedBookies, readonlyBookies);
            } else {
                assertEquals(0, readonlyBookies.size());
                assertEquals(numBookies, writableBookies.size());
                assertEquals(expectedBookies, writableBookies);
            }

        });
    }

    @Test
    public void testWatchWritableBookies() throws Exception {
        testWatchBookies(false);
    }

    @Test
    public void testWatchReadonlyBookies() throws Exception {
        testWatchBookies(true);
    }

    private void testWatchBookies(boolean readonly) throws Exception {
        LinkedBlockingQueue<Versioned<Set<BookieSocketAddress>>> writableChanges = new LinkedBlockingQueue<>();
        LinkedBlockingQueue<Versioned<Set<BookieSocketAddress>>> readonlyChanges = new LinkedBlockingQueue<>();
        result(regClient.watchReadOnlyBookies(newRegistrationListener(readonlyChanges)));
        result(regClient.watchWritableBookies(newRegistrationListener(writableChanges)));
        Versioned<Set<BookieSocketAddress>> versionedBookies = writableChanges.take();
        assertTrue(versionedBookies.getValue().isEmpty());
        versionedBookies = readonlyChanges.take();
        assertTrue(versionedBookies.getValue().isEmpty());

        final int numBookies = 3;
        final List<EtcdRegistrationManager> bookies = createNumBookies(readonly, numBookies, scope, 1);

        LinkedBlockingQueue<Versioned<Set<BookieSocketAddress>>> changes;
        if (readonly) {
            changes = readonlyChanges;
        } else {
            changes = writableChanges;
        }

        Version preVersion = new LongVersion(-1);
        Set<BookieSocketAddress> expectedBookies = new HashSet<>();
        for (int i = 0; i < numBookies; i++) {
            BookieSocketAddress address = new BookieSocketAddress(newBookie(i));
            expectedBookies.add(address);

            versionedBookies = changes.take();
            Version curVersion = versionedBookies.getVersion();
            assertEquals(Occurred.AFTER, curVersion.compare(preVersion));
            assertEquals(expectedBookies, versionedBookies.getValue());
            preVersion = curVersion;
        }

        bookies.forEach(EtcdRegistrationManager::close);
        for (int i = 0; i < numBookies; i++) {
            versionedBookies = changes.take();
            Version curVersion = versionedBookies.getVersion();
            assertEquals(Occurred.AFTER, curVersion.compare(preVersion));
            assertEquals(numBookies - i - 1, versionedBookies.getValue().size());
            preVersion = curVersion;
        }
        if (readonly) {
            assertEquals(0, writableChanges.size());
        } else {
            assertEquals(0, readonlyChanges.size());
        }
    }

    private static List<EtcdRegistrationManager> createNumBookies(boolean readonly,
                                                                  int numBookies,
                                                                  String scope,
                                                                  long ttlSeconds) throws BookieException {
        List<EtcdRegistrationManager> bookies = new ArrayList<>(numBookies);
        for (int i = 0; i < numBookies; i++) {
            Client client = newEtcdClient();
            EtcdRegistrationManager regMgr = new EtcdRegistrationManager(client, scope, ttlSeconds);
            bookies.add(regMgr);
            regMgr.registerBookie(newBookie(i), readonly, BookieServiceInfo.EMPTY);
        }
        return bookies;
    }

    private static List<EtcdRegistrationManager> createNumBookies(boolean readonly,
                                                                  int numBookies,
                                                                  String scope) throws BookieException {
        return createNumBookies(readonly, numBookies, scope, 60);
    }

    @Test
    public void testRegisterBookieWaitUntilPreviousExpiredSuccess() throws Exception {
        long ttlSeconds = 1;
        long leaseId = -0xabcd;
        String bookieId = runtime.getMethodName() + ":3181";
        try (EtcdRegistrationManager regManager = new EtcdRegistrationManager(
            newEtcdClient(), scope, ttlSeconds)
        ) {
            regManager.registerBookie(bookieId, false, BookieServiceInfo.EMPTY);
            leaseId = regManager.getBkRegister().getLeaseId();
            log.info("Registered bookie under scope '{}' with lease = {}", scope, leaseId);
        }
        assertNotEquals(-0xabcd, leaseId);
        final long prevLeaseId = leaseId;
        try (EtcdRegistrationManager regManager = new EtcdRegistrationManager(
            newEtcdClient(), scope, 100000 * ttlSeconds)
        ) {
            regManager.registerBookie(bookieId, false, BookieServiceInfo.EMPTY);
            leaseId = regManager.getBkRegister().getLeaseId();
            log.info("Registered bookie under scope '{}' with new lease = {}", scope, leaseId);
        }
        assertNotEquals(prevLeaseId, leaseId);
    }

    @Test
    public void testRegisterBookieWaitUntilPreviousExpiredFailure() throws Exception {
        long ttlSeconds = 1;
        long leaseId = -0xabcd;
        String bookieId = runtime.getMethodName() + ":3181";
        try (EtcdRegistrationManager regManager = new EtcdRegistrationManager(
            newEtcdClient(), scope, 10000000 * ttlSeconds)
        ) {
            regManager.registerBookie(bookieId, false, BookieServiceInfo.EMPTY);
            leaseId = regManager.getBkRegister().getLeaseId();
            log.info("Registered bookie under scope '{}' with lease = {}", scope, leaseId);
        }
        assertNotEquals(-0xabcd, leaseId);
        try (EtcdRegistrationManager regManager = new EtcdRegistrationManager(
            newEtcdClient(), scope,  ttlSeconds)
        ) {
            regManager.registerBookie(bookieId, false, BookieServiceInfo.EMPTY);
            fail("Should fail to register bookie under scope '{}'"
                + " since previous registration has not been expired yet");
        } catch (MetadataStoreException mse) {
            log.info("Encountered exception on registering bookie under scope '{}'", scope, mse);
            // expected
        }
    }

    @Test
    public void testRegisterWritableBookieWithSameLeaseId() throws Exception {
        testRegisterBookieWithSameLeaseId(false);
    }

    @Test
    public void testRegisterReadonlyBookieWithSameLeaseId() throws Exception {
        testRegisterBookieWithSameLeaseId(true);
    }

    private void testRegisterBookieWithSameLeaseId(boolean readonly) throws Exception {
        long ttlSeconds = 1;
        long leaseId = -0xabcd;
        String bookieId = runtime.getMethodName() + ":3181";
        try (EtcdRegistrationManager regManager = new EtcdRegistrationManager(
            newEtcdClient(), scope, 10000000 * ttlSeconds)
        ) {
            regManager.registerBookie(bookieId, readonly, BookieServiceInfo.EMPTY);
            leaseId = regManager.getBkRegister().getLeaseId();
            log.info("Registered bookie under scope '{}' with lease = {}", scope, leaseId);
            log.info("Trying to register using same lease '{}'", leaseId);
            try (EtcdRegistrationManager regManager2 = new EtcdRegistrationManager(
                regManager.getClient(), scope, regManager.getBkRegister()
            )) {
                regManager.registerBookie(bookieId, readonly, BookieServiceInfo.EMPTY);
            }
        }
    }

    private Set<BookieSocketAddress> getBookies(boolean readonly) throws Exception {
        Set<BookieSocketAddress> bookies;
        if (readonly) {
            bookies = result(regClient.getReadOnlyBookies()).getValue();
        } else {
            bookies = result(regClient.getWritableBookies()).getValue();
        }
        return bookies;
    }

    @Test
    public void testRegisterUnregisterWritableBookie() throws Exception {
        testRegisterUnregister(false);
    }

    @Test
    public void testRegisterUnregisterReadonlyBookie() throws Exception {
        testRegisterUnregister(true);
    }

    private void testRegisterUnregister(boolean readonly) throws Exception {
        String bookieId = runtime.getMethodName();
        if (readonly) {
            bookieId += "-readonly";
        }
        bookieId += ":3181";
        try (EtcdRegistrationManager regMgr = new EtcdRegistrationManager(
            newEtcdClient(), scope, 1000000000
        )) {
            // before registration
            Set<BookieSocketAddress> bookies = getBookies(readonly);
            log.info("before registration : bookies = {}", bookies);
            assertEquals(0, bookies.size());
            // registered
            regMgr.registerBookie(bookieId, readonly, BookieServiceInfo.EMPTY);
            bookies = getBookies(readonly);
            log.info("after registered: bookies = {}", bookies);
            assertEquals(1, bookies.size());
            assertEquals(
                Sets.newHashSet(new BookieSocketAddress(bookieId)),
                bookies);
            // unregistered
            regMgr.unregisterBookie(bookieId, readonly);
            bookies = getBookies(readonly);
            log.info("after unregistered: bookies = {}", bookies);
            assertEquals(0, bookies.size());
        }
    }

    @Test
    public void testConcurrentWritableRegistration() throws Exception {
        testConcurrentRegistration(false);
    }

    @Test
    public void testConcurrentReadonlyRegistration() throws Exception {
        testConcurrentRegistration(true);
    }

    private void testConcurrentRegistration(boolean readonly) throws Exception {
        final String bookieId;
        if (readonly) {
            bookieId = runtime.getMethodName() + "-readonly:3181";
        } else {
            bookieId = runtime.getMethodName() + ":3181";
        }
        final int numBookies = 10;
        @Cleanup("shutdown")
        ExecutorService executor = Executors.newFixedThreadPool(numBookies);
        final CyclicBarrier startBarrier = new CyclicBarrier(numBookies);
        final CyclicBarrier completeBarrier = new CyclicBarrier(numBookies);
        final CompletableFuture<Void> doneFuture = new CompletableFuture<>();
        final AtomicInteger numSuccesses = new AtomicInteger(0);
        final AtomicInteger numFailures = new AtomicInteger(0);
        for (int i = 0; i < numBookies; i++) {
            executor.submit(() -> {
                try (EtcdRegistrationManager regMgr = new EtcdRegistrationManager(
                    newEtcdClient(), scope, 1
                )) {
                    try {
                        startBarrier.await();
                        regMgr.registerBookie(bookieId, readonly, BookieServiceInfo.EMPTY);
                        numSuccesses.incrementAndGet();
                    } catch (InterruptedException e) {
                        log.warn("Interrupted at waiting for the other threads to start", e);
                    } catch (BrokenBarrierException e) {
                        log.warn("Start barrier is broken", e);
                    } catch (BookieException e) {
                        numFailures.incrementAndGet();
                    }
                    try {
                        completeBarrier.await();
                    } catch (InterruptedException e) {
                        log.warn("Interrupted at waiting for the other threads to complete", e);
                    } catch (BrokenBarrierException e) {
                        log.warn("Complete barrier is broken", e);
                    }
                    FutureUtils.complete(doneFuture, null);
                }
            });
        }
        doneFuture.join();
        assertEquals(1, numSuccesses.get());
        assertEquals(numBookies - 1, numFailures.get());
    }

}
