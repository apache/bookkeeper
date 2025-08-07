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
package org.apache.bookkeeper.discover;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.junit.Test;

public class ZKRegistrationClientTest extends BookKeeperClusterTestCase {

    public ZKRegistrationClientTest() {
        super(0);
    }

    @Test
    public void testNetworkDelayWithBkZkManager() throws Throwable {
        final String zksConnectionString = zkUtil.getZooKeeperConnectString();
        final String ledgersRoot = "/test/ledgers-" + UUID.randomUUID();
        // prepare registration manager
        @Cleanup
        ZooKeeper zk = new ZooKeeper(zksConnectionString, 5000, null);
        final ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setZkLedgersRootPath(ledgersRoot);
        final FaultInjectableZKRegistrationManager rm =
                new FaultInjectableZKRegistrationManager(serverConfiguration, zk);
        rm.prepareFormat();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        @Cleanup
        RegistrationClient rc1 = new ZKRegistrationClient(zk, ledgersRoot, scheduledExecutorService, true);
        rc1.watchWritableBookies(bookies -> {
            System.out.println("Writable bookies changed" + bookies);
        });
        rc1.watchReadOnlyBookies(bookies -> {
            System.out.println("ReadOnly bookies changed" + bookies);
        });
        @Cleanup
        RegistrationClient rc2 = new ZKRegistrationClient(zk, ledgersRoot, scheduledExecutorService, true);
        rc2.watchWritableBookies(bookies -> {
            System.out.println("Writable bookies changed");
        });
        rc2.watchReadOnlyBookies(bookies -> {
            System.out.println("ReadOnly bookies changed");
        });

        final List<BookieId> addresses = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            addresses.add(BookieId.parse("BOOKIE-" + i));
        }
        final Map<BookieId, BookieServiceInfo> bookieServiceInfos = new HashMap<>();

        int port = 223;
        for (BookieId address : addresses) {
            BookieServiceInfo info = new BookieServiceInfo();
            BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
            endpoint.setAuth(Collections.emptyList());
            endpoint.setExtensions(Collections.emptyList());
            endpoint.setId("id");
            endpoint.setHost("localhost");
            endpoint.setPort(port++);
            endpoint.setProtocol("bookie-rpc");
            info.setEndpoints(Arrays.asList(endpoint));
            bookieServiceInfos.put(address, info);
            rm.registerBookie(address, false, info);
            // write the cookie
            rm.writeCookie(address, new Versioned<>(new byte[0], Version.NEW));
        }

        // trigger loading the BookieServiceInfo in the local cache
        getAndVerifyAllBookies(rc1, addresses);
        getAndVerifyAllBookies(rc2, addresses);

        Awaitility.await().untilAsserted(() -> {
            for (BookieId address : addresses) {
                compareBookieServiceInfo(rc1.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
                compareBookieServiceInfo(rc2.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
            }
        });

        // verified the init status.


        // mock network delay
        rm.betweenRegisterReadOnlyBookie(__ -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return null;
        });

        for (int i = 0; i < addresses.size() / 2; i++) {
            final BookieId bkId = addresses.get(i);
            // turn some bookies to be read only.
            rm.registerBookie(bkId, true, bookieServiceInfos.get(bkId));
        }

        Awaitility.await().untilAsserted(() -> {
            for (BookieId address : addresses) {
                compareBookieServiceInfo(rc1.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
                compareBookieServiceInfo(rc2.getBookieServiceInfo(address).get().getValue(),
                        bookieServiceInfos.get(address));
            }
        });

    }

    private static void getAndVerifyAllBookies(RegistrationClient rc, List<BookieId> addresses)
            throws InterruptedException, ExecutionException {
        Set<BookieId> all = rc.getAllBookies().get().getValue();
        assertEquals(all.size(), addresses.size());
        for (BookieId id : all) {
            assertTrue(addresses.contains(id));
        }
        for (BookieId id : addresses) {
            assertTrue(all.contains(id));
        }
    }

    private void compareBookieServiceInfo(BookieServiceInfo a, BookieServiceInfo b) {
        assertEquals(a.getProperties(), b.getProperties());
        assertEquals(a.getEndpoints().size(), b.getEndpoints().size());
        for (int i = 0; i < a.getEndpoints().size(); i++) {
            BookieServiceInfo.Endpoint e1 = a.getEndpoints().get(i);
            BookieServiceInfo.Endpoint e2 = b.getEndpoints().get(i);
            assertEquals(e1.getHost(), e2.getHost());
            assertEquals(e1.getPort(), e2.getPort());
            assertEquals(e1.getId(), e2.getId());
            assertEquals(e1.getProtocol(), e2.getProtocol());
            assertEquals(e1.getExtensions(), e2.getExtensions());
            assertEquals(e1.getAuth(), e2.getAuth());
        }

    }
}
