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

package org.apache.bookkeeper.zookeeper;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.netty.util.NetUtil;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.zookeeper.ZKTestCase;
import org.apache.zookeeper.client.ConnectStringParser;
import org.apache.zookeeper.client.HostProvider;
import org.apache.zookeeper.client.StaticHostProvider;
import org.apache.zookeeper.common.Time;
import org.burningwave.tools.net.DefaultHostResolver;
import org.burningwave.tools.net.HostResolutionRequestInterceptor;
import org.burningwave.tools.net.HostResolver;
import org.burningwave.tools.net.IPAddressUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CustomZooKeeperHostProviderTest extends ZKTestCase {

    @BeforeAll
    public static void setupDNSMocks() {
        MultiMappedHostResolver hostResolver = new MultiMappedHostResolver();
        hostResolver.putHost("site1.mock", "192.168.1.1");
        hostResolver.putHost("site2.mock", "192.168.1.2");
        hostResolver.putHost("site3.mock", "192.168.1.3");
        hostResolver.putHost("site4.mock", "192.168.1.4");
        for (int i = 1; i <= 10; i++) {
            // each testhost-*.mock has 2 ip addresses so that certain scenarios can be tested
            hostResolver.putHost(String.format("testhost-%d.mock", i), String.format("10.2.3.%d", i),
                    String.format("10.50.50.%d", i));
            // add same hostnames and ip addresses with different domain
            hostResolver.putHost(String.format("testhost-%d.othermock", i), String.format("10.2.3.%d", i),
                    String.format("10.50.50.%d", i));
        }

        HostResolutionRequestInterceptor.INSTANCE.install(
                hostResolver,
                DefaultHostResolver.INSTANCE
        );
    }

    @AfterAll
    public static void clearDNSMocks() {
        HostResolutionRequestInterceptor.INSTANCE.uninstall();
    }

    private Random r = new Random(1);

    @Test
    public void testNextGoesRound() {
        HostProvider hostProvider = getHostProvider((byte) 2);
        InetSocketAddress first = hostProvider.next(0);
        assertTrue(first != null);
        hostProvider.next(0);
        assertEquals(first, hostProvider.next(0));
    }

    @Test
    public void testNextGoesRoundAndSleeps() {
        byte size = 2;
        HostProvider hostProvider = getHostProvider(size);
        while (size > 0) {
            hostProvider.next(0);
            --size;
        }
        long start = Time.currentElapsedTime();
        hostProvider.next(1000);
        long stop = Time.currentElapsedTime();
        assertTrue(900 <= stop - start);
    }

    @Test
    public void testNextDoesNotSleepForZero() {
        byte size = 2;
        HostProvider hostProvider = getHostProvider(size);
        while (size > 0) {
            hostProvider.next(0);
            --size;
        }
        long start = Time.currentElapsedTime();
        hostProvider.next(0);
        long stop = Time.currentElapsedTime();
        assertTrue(5 > stop - start);
    }

    @Test
    public void testEmptyServerAddressesList() {
        assertThrows(IllegalArgumentException.class, () -> {
            HostProvider hp = new CustomZooKeeperHostProvider(new ArrayList<>());
        });
    }

    @Test
    public void testInvalidHostAddresses() {
        // Arrange
        final List<InetSocketAddress> invalidAddresses = new ArrayList<>();
        InetSocketAddress unresolved = InetSocketAddress.createUnresolved("a", 1234);
        invalidAddresses.add(unresolved);
        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) throws UnknownHostException {
                throw new UnknownHostException();
            }
        };
        CustomZooKeeperHostProvider sp = new CustomZooKeeperHostProvider(invalidAddresses, resolver);

        // Act & Assert
        InetSocketAddress n1 = sp.next(0);
        assertTrue(n1.isUnresolved(), "Provider should return unresolved address is host is unresolvable");
        assertSame(unresolved, n1, "Provider should return original address is host is unresolvable");
    }

    @Test
    public void testTwoConsequitiveCallsToNextReturnDifferentElement() {
        HostProvider hostProvider = getHostProvider((byte) 2);
        assertNotSame(hostProvider.next(0), hostProvider.next(0));
    }

    @Test
    public void testOnConnectDoesNotReset() {
        HostProvider hostProvider = getHostProvider((byte) 2);
        InetSocketAddress first = hostProvider.next(0);
        hostProvider.onConnected();
        InetSocketAddress second = hostProvider.next(0);
        assertNotSame(first, second);
    }

    /* Reconfig tests with IP addresses */

    private final double slackPercent = 10;
    private final int numClients = 10000;

    @Test
    public void testUpdateClientMigrateOrNot() throws UnknownHostException {
        HostProvider hostProvider =
                getHostProvider((byte) 4); // 10.10.10.4:1238, 10.10.10.3:1237, 10.10.10.2:1236, 10.10.10.1:1235
        Collection<InetSocketAddress> newList =
                getServerAddresses((byte) 3); // 10.10.10.3:1237, 10.10.10.2:1236, 10.10.10.1:1235

        InetSocketAddress myServer = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, 3}), 1237);

        // Number of machines becomes smaller, my server is in the new cluster
        boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertFalse(disconnectRequired);
        hostProvider.onConnected();

        // Number of machines stayed the same, my server is in the new cluster
        disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertFalse(disconnectRequired);
        hostProvider.onConnected();

        // Number of machines became smaller, my server is not in the new
        // cluster
        newList = getServerAddresses((byte) 2); // 10.10.10.2:1236, 10.10.10.1:1235
        disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertTrue(disconnectRequired);
        hostProvider.onConnected();

        // Number of machines stayed the same, my server is not in the new
        // cluster
        disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertTrue(disconnectRequired);
        hostProvider.onConnected();

        // Number of machines increased, my server is not in the new cluster
        newList = new ArrayList<>(3);
        for (byte i = 4; i > 1; i--) { // 10.10.10.4:1238, 10.10.10.3:1237, 10.10.10.2:1236
            newList.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }
        myServer = new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, 1}), 1235);
        disconnectRequired = hostProvider.updateServerList(newList, myServer);
        assertTrue(disconnectRequired);
        hostProvider.onConnected();

        // Number of machines increased, my server is in the new cluster
        // Here whether to move or not depends on the difference of cluster
        // sizes
        // With probability 1 - |old|/|new} the client disconnects
        // In the test below 1-9/10 = 1/10 chance of disconnecting
        HostProvider[] hostProviderArray = new HostProvider[numClients];
        newList = getServerAddresses((byte) 10);
        int numDisconnects = 0;
        for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            disconnectRequired = hostProviderArray[i].updateServerList(newList, myServer);
            if (disconnectRequired) {
                numDisconnects++;
            }
        }
        hostProvider.onConnected();

        // should be numClients/10 in expectation, we test that its numClients/10 +- slackPercent
        assertTrue(numDisconnects < upperboundCPS(numClients, 10));
    }

    @Test
    public void testUpdateMigrationGoesRound() throws UnknownHostException {
        HostProvider hostProvider = getHostProvider((byte) 4);
        // old list (just the ports): 1238, 1237, 1236, 1235
        Collection<InetSocketAddress> newList = new ArrayList<>(10);
        for (byte i = 12; i > 2; i--) { // 1246, 1245, 1244, 1243, 1242, 1241,
            // 1240, 1239, 1238, 1237
            newList.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

        // servers from the old list that appear in the new list
        Collection<InetSocketAddress> oldStaying = new ArrayList<>(2);
        for (byte i = 4; i > 2; i--) { // 1238, 1237
            oldStaying.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

        // servers in the new list that are not in the old list
        Collection<InetSocketAddress> newComing = new ArrayList<>(10);
        for (byte i = 12; i > 4; i--) {// 1246, 1245, 1244, 1243, 1242, 1241, 1240, 1139
            newComing.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

        // Number of machines increases, my server is not in the new cluster
        // load on old servers must be decreased, so must connect to one of the
        // new servers
        // i.e., pNew = 1.

        boolean disconnectRequired = hostProvider.updateServerList(newList,
                new InetSocketAddress(InetAddress.getByAddress(new byte[] {10, 10, 10, 1}), 1235));
        assertTrue(disconnectRequired);

        // This means reconfigMode = true, and nextHostInReconfigMode will be
        // called from next
        // Since pNew = 1 we should first try the new servers
        ArrayList<InetSocketAddress> seen = new ArrayList<>();
        for (int i = 0; i < newComing.size(); i++) {
            InetSocketAddress addr = hostProvider.next(0);
            assertTrue(newComing.contains(addr));
            assertTrue(!seen.contains(addr));
            seen.add(addr);
        }

        // Next the old servers
        seen.clear();
        for (int i = 0; i < oldStaying.size(); i++) {
            InetSocketAddress addr = hostProvider.next(0);
            assertTrue(oldStaying.contains(addr));
            assertTrue(!seen.contains(addr));
            seen.add(addr);
        }

        // And now it goes back to normal next() so it should be everything
        // together like in testNextGoesRound()
        InetSocketAddress first = hostProvider.next(0);
        assertTrue(first != null);
        for (int i = 0; i < newList.size() - 1; i++) {
            hostProvider.next(0);
        }

        assertEquals(first, hostProvider.next(0));
        hostProvider.onConnected();
    }

    @Test
    public void testUpdateLoadBalancing() throws UnknownHostException {
        // Start with 9 servers and 10000 clients
        boolean disconnectRequired;
        HostProvider[] hostProviderArray = new HostProvider[numClients];
        InetSocketAddress[] curHostForEachClient = new InetSocketAddress[numClients];
        int[] numClientsPerHost = new int[9];

        // initialization
        for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            curHostForEachClient[i] = hostProviderArray[i].next(0);
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 9));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 9));
            numClientsPerHost[i] = 0; // prepare for next test
        }

        // remove host number 8 (the last one in a list of 9 hosts)
        Collection<InetSocketAddress> newList = getServerAddresses((byte) 8);

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 8; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 8));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 8));
            numClientsPerHost[i] = 0; // prepare for next test
        }
        assertTrue(numClientsPerHost[8] == 0);

        // remove hosts number 6 and 7 (the currently last two in the list)
        newList = getServerAddresses((byte) 6);

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 6; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 6));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 6));
            numClientsPerHost[i] = 0; // prepare for next test
        }
        assertTrue(numClientsPerHost[6] == 0);
        assertTrue(numClientsPerHost[7] == 0);
        assertTrue(numClientsPerHost[8] == 0);

        // remove host number 0 (the first one in the current list)
        // and add back hosts 6, 7 and 8
        newList = new ArrayList<>(8);
        for (byte i = 9; i > 1; i--) {
            newList.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, i}), 1234 + i));
        }

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        assertTrue(numClientsPerHost[0] == 0);

        for (int i = 1; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 8));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 8));
            numClientsPerHost[i] = 0; // prepare for next test
        }

        // add back host number 0
        newList = getServerAddresses((byte) 9);

        for (int i = 0; i < numClients; i++) {
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 9));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 9));
        }
    }

    @Test
    public void testNoCurrentHostDuringNormalMode() throws UnknownHostException {
        // Start with 9 servers and 10000 clients
        boolean disconnectRequired;
        CustomZooKeeperHostProvider[] hostProviderArray = new CustomZooKeeperHostProvider[numClients];
        InetSocketAddress[] curHostForEachClient = new InetSocketAddress[numClients];
        int[] numClientsPerHost = new int[9];

        // initialization
        for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            if (i >= (numClients / 2)) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            } else {
                // its supposed to be the first server on serverList.
                // we'll set it later, see below (*)
                curHostForEachClient[i] = null;
            }
        }

        // remove hosts 7 and 8 (the last two in a list of 9 hosts)
        Collection<InetSocketAddress> newList = getServerAddresses((byte) 7);

        for (int i = 0; i < numClients; i++) {
            // tests the case currentHost == null && lastIndex == -1
            // calls next for clients with index < numClients/2
            disconnectRequired = hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
            if (disconnectRequired) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            } else if (curHostForEachClient[i] == null) {
                // (*) setting it to what it should be
                curHostForEachClient[i] = hostProviderArray[i].getServerAtIndex(0);
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            // sets lastIndex, resets reconfigMode
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 7; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 7));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 7));
            numClientsPerHost[i] = 0; // prepare for next test
        }
        assertTrue(numClientsPerHost[7] == 0);
        assertTrue(numClientsPerHost[8] == 0);

        // add back server 7
        newList = getServerAddresses((byte) 8);

        for (int i = 0; i < numClients; i++) {
            InetSocketAddress myServer = (i < (numClients / 2)) ? null : curHostForEachClient[i];
            // tests the case currentHost == null && lastIndex >= 0
            disconnectRequired = hostProviderArray[i].updateServerList(newList, myServer);
            if (disconnectRequired) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 8; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 8));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 8));
        }
    }

    @Test
    public void testReconfigDuringReconfigMode() throws UnknownHostException {
        // Start with 9 servers and 10000 clients
        boolean disconnectRequired;
        CustomZooKeeperHostProvider[] hostProviderArray = new CustomZooKeeperHostProvider[numClients];
        InetSocketAddress[] curHostForEachClient = new InetSocketAddress[numClients];
        int[] numClientsPerHost = new int[9];

        // initialization
        for (int i = 0; i < numClients; i++) {
            hostProviderArray[i] = getHostProvider((byte) 9);
            curHostForEachClient[i] = hostProviderArray[i].next(0);
        }

        // remove hosts 7 and 8 (the last two in a list of 9 hosts)
        Collection<InetSocketAddress> newList = getServerAddresses((byte) 7);

        for (int i = 0; i < numClients; i++) {
            // sets reconfigMode
            hostProviderArray[i].updateServerList(newList, curHostForEachClient[i]);
        }

        // add back servers 7 and 8 while still in reconfigMode (we didn't call
        // next)
        newList = getServerAddresses((byte) 9);

        for (int i = 0; i < numClients; i++) {
            InetSocketAddress myServer = (i < (numClients / 2)) ? null : curHostForEachClient[i];
            // for i < (numClients/2) this tests the case currentHost == null &&
            // reconfigMode = true
            // for i >= (numClients/2) this tests the case currentHost!=null &&
            // reconfigMode = true
            disconnectRequired = hostProviderArray[i].updateServerList(newList, myServer);
            if (disconnectRequired) {
                curHostForEachClient[i] = hostProviderArray[i].next(0);
            } else {
                // currentIndex was set by the call to updateServerList, which
                // called next
                curHostForEachClient[i] = hostProviderArray[i].getServerAtCurrentIndex();
            }
            numClientsPerHost[curHostForEachClient[i].getPort() - 1235]++;
            hostProviderArray[i].onConnected();
        }

        for (int i = 0; i < 9; i++) {
            assertTrue(numClientsPerHost[i] <= upperboundCPS(numClients, 9));
            assertTrue(numClientsPerHost[i] >= lowerboundCPS(numClients, 9));
        }
    }

    private CustomZooKeeperHostProvider getHostProvider(byte size) {
        return new CustomZooKeeperHostProvider(getServerAddresses(size), r.nextLong());
    }

    private Collection<InetSocketAddress> getServerAddresses(byte size) {
        ArrayList<InetSocketAddress> list = new ArrayList<>(size);
        while (size > 0) {
            try {
                list.add(new InetSocketAddress(InetAddress.getByAddress(new byte[]{10, 10, 10, size}), 1234 + size));
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            --size;
        }
        return list;
    }

    /* Reconfig test with unresolved hostnames. */

    /**
     * Number of machines becomes smaller, my server is in the new cluster.
     */
    @Test
    public void testUpdateServerList_UnresolvedHostnames_NoDisconnection1() {
        // Arrange
        // [testhost-4.mock:1238, testhost-3.mock:1237, testhost-2.mock:1236,
        // testhost-1.mock:1235]
        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(4);
        // [testhost-3.mock:1237, testhost-2.mock:1236, testhost-1.mock:1235]
        Collection<InetSocketAddress> newList = getUnresolvedHostnames(3);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-3.mock", 1237);

        // Act
        boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

        // Assert
        assertFalse(disconnectRequired);
        hostProvider.onConnected();
    }

    /**
     * Number of machines stayed the same, my server is in the new cluster.
     */
    @Test
    public void testUpdateServerList_UnresolvedHostnames_NoDisconnection2() {
        // Arrange
        // [testhost-3.mock:1237, testhost-2.mock:1236, testhost-1.mock:1235]
        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(3);
        // [testhost-3.mock:1237, testhost-2.mock:1236, testhost-1.mock:1235]
        Collection<InetSocketAddress> newList = getUnresolvedHostnames(3);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-3.mock", 1237);

        // Act
        boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

        // Assert
        assertFalse(disconnectRequired);
        hostProvider.onConnected();
    }

    /**
     * Number of machines stayed the same, my server is in the new cluster.
     */
    @Test
    public void testUpdateServerList_MultipleIPs_and_Domains_NoDisconnection() {
        // Arrange
        // [testhost-3.mock:1237, testhost-2.mock:1236, testhost-1.mock:1235]
        ArrayList<InetSocketAddress> serverAddresses = new ConnectStringParser(
                "testhost-1.mock:1235,testhost-2.mock:1236,testhost-3.mock:1237").getServerAddresses();
        HostProvider hostProvider = new CustomZooKeeperHostProvider(serverAddresses, 9);

        // Change one server
        List<InetSocketAddress> newList = new ConnectStringParser(
                "testhost-1.mock:1235,testhost-2.othermock:1236,testhost-3.mock:1237").getServerAddresses();

        // use resolved remote server address (similar cause with sockets)
        InetSocketAddress myServer =
                new InetSocketAddress(NetUtil.createInetAddressFromIpAddressString("10.50.50.2"), 1236);

        // Act
        boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

        // Assert
        assertFalse(disconnectRequired);
        hostProvider.onConnected();
    }

    /**
     * Number of machines became smaller, my server is not in the new cluster.
     */
    @Test
    public void testUpdateServerList_UnresolvedHostnames_Disconnection1() {
        // Arrange
        // [testhost-3.mock:1237, testhost-2.mock:1236, testhost-1.mock:1235]
        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(3);
        // [testhost-2.mock:1236, testhost-1.mock:1235]
        Collection<InetSocketAddress> newList = getUnresolvedHostnames(2);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-3.mock", 1237);

        // Act
        boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

        // Assert
        assertTrue(disconnectRequired);
        hostProvider.onConnected();
    }

    /**
     * Number of machines stayed the same, my server is not in the new cluster.
     */
    @Test
    public void testUpdateServerList_UnresolvedHostnames_Disconnection2() {
        // Arrange
        // [testhost-3.mock:1237, testhost-2.mock:1236, testhost-1.mock:1235]
        HostProvider hostProvider = getHostProviderWithUnresolvedHostnames(3);
        // [testhost-3.mock:1237, testhost-2.mock:1236, testhost-1.mock:1235]
        Collection<InetSocketAddress> newList = getUnresolvedHostnames(3);
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-4.mock", 1237);

        // Act
        boolean disconnectRequired = hostProvider.updateServerList(newList, myServer);

        // Assert
        assertTrue(disconnectRequired);
        hostProvider.onConnected();
    }

    @Test
    public void testUpdateServerList_ResolvedWithUnResolvedAddress_ForceDisconnect() {
        // Arrange
        // Create a HostProvider with a list of unresolved server address(es)
        List<InetSocketAddress> addresses =
                Collections.singletonList(InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235));
        HostProvider hostProvider = new CustomZooKeeperHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost is which the client is currently connecting to, it should be resolved",
                currentHost.isUnresolved(), is(false));

        // Act
        InetSocketAddress replaceHost = InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235);
        assertThat("Replace host must be unresolved in this test case", replaceHost.isUnresolved(), is(true));
        boolean disconnect =
                hostProvider.updateServerList(new ArrayList<>(Collections.singletonList(replaceHost)), currentHost);

        // Assert
        assertThat(disconnect, is(false));
    }

    @Test
    public void testUpdateServerList_ResolvedWithResolvedAddress_NoDisconnect() throws UnknownHostException {
        // Arrange
        // Create a HostProvider with a list of unresolved server address(es)
        List<InetSocketAddress> addresses =
                Collections.singletonList(InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235));
        HostProvider hostProvider = new CustomZooKeeperHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost is which the client is currently connecting to, it should be resolved",
                currentHost.isUnresolved(), is(false));

        // Act
        InetSocketAddress replaceHost = new InetSocketAddress(
                InetAddress.getByAddress(currentHost.getHostString(), currentHost.getAddress().getAddress()),
                currentHost.getPort());
        assertThat("Replace host must be resolved in this test case", replaceHost.isUnresolved(), is(false));
        boolean disconnect =
                hostProvider.updateServerList(new ArrayList<>(Collections.singletonList(replaceHost)), currentHost);

        // Assert
        assertThat(disconnect, equalTo(false));
    }

    @Test
    public void testUpdateServerList_UnResolvedWithUnResolvedAddress_ForceDisconnect() {
        // Arrange
        // Create a HostProvider with a list of unresolved server address(es)
        List<InetSocketAddress> addresses =
                Collections.singletonList(InetSocketAddress.createUnresolved("testhost-1.zookeepertest.zk", 1235));
        HostProvider hostProvider = new CustomZooKeeperHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost is not resolvable in this test case", currentHost.isUnresolved(), is(true));

        // Act
        InetSocketAddress replaceHost = InetSocketAddress.createUnresolved("testhost-1.resolvable.zk", 1235);
        assertThat("Replace host must be unresolved in this test case", replaceHost.isUnresolved(), is(true));
        boolean disconnect =
                hostProvider.updateServerList(new ArrayList<>(Collections.singletonList(replaceHost)), currentHost);

        // Assert
        assertThat(disconnect, is(true));
    }

    @Test
    public void testUpdateServerList_UnResolvedWithResolvedAddress_ForceDisconnect() throws UnknownHostException {
        // Arrange
        // Create a HostProvider with a list of unresolved server address(es)
        List<InetSocketAddress> addresses =
                Collections.singletonList(InetSocketAddress.createUnresolved("testhost-1.zookeepertest.zk", 1235));
        HostProvider hostProvider = new CustomZooKeeperHostProvider(addresses, new TestResolver());
        InetSocketAddress currentHost = hostProvider.next(100);
        assertThat("CurrentHost not resolvable in this test case", currentHost.isUnresolved(), is(true));

        // Act
        byte[] addr = new byte[] {10, 0, 0, 1};
        InetSocketAddress replaceHost =
                new InetSocketAddress(InetAddress.getByAddress(currentHost.getHostString(), addr),
                        currentHost.getPort());
        assertThat("Replace host must be resolved in this test case", replaceHost.isUnresolved(), is(false));
        boolean disconnect =
                hostProvider.updateServerList(new ArrayList<>(Collections.singletonList(replaceHost)), currentHost);

        // Assert
        assertThat(disconnect, equalTo(false));
    }

    // ==================== isMyServerInNewConfig tests ====================

    /**
     * Builds a Resolver backed by a map; names not in the map throw UnknownHostException.
     */
    private static StaticHostProvider.Resolver mapResolver(Map<String, InetAddress[]> map) {
        return name -> {
            InetAddress[] result = map.get(name);
            if (result == null) {
                throw new UnknownHostException(name);
            }
            return result;
        };
    }

    /**
     * Port mismatch: server hostname matches but port differs → not found.
     */
    @Test
    public void testIsMyServerInNewConfig_PortMismatch() {
        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.mock", 2181));
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-1.mock", 2182);

        assertFalse(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(Collections.emptyMap()), connectAddresses, myServer));
    }

    /**
     * Exact hostname string match (branch 1): connect hostname == myServer hostname.
     */
    @Test
    public void testIsMyServerInNewConfig_HostnameStringMatch() {
        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.mock", 2181));
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-1.mock", 2181);

        assertTrue(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(Collections.emptyMap()), connectAddresses, myServer));
    }

    /**
     * IP string match (branch 2): connect hostname is an IP literal that matches myServer's resolved IP.
     */
    @Test
    public void testIsMyServerInNewConfig_IPStringMatch() throws UnknownHostException {
        // Arrange
        byte[] ip = {10, 2, 3, 1};
        InetAddress resolvedIP = InetAddress.getByAddress("testhost-1.mock", ip);
        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("10.2.3.1", 2181));
        InetSocketAddress myServer = new InetSocketAddress(resolvedIP, 2181);

        assertTrue(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(Collections.emptyMap()), connectAddresses, myServer));
    }

    /**
     * Resolved connect-address IP matches myServer's IP (branch 3).
     * Also serves as a regression test for the NPE fix: when the resolver does return addresses,
     * the loop must not throw NullPointerException.
     */
    @Test
    public void testIsMyServerInNewConfig_ResolvedConnectIPMatchesMyServerIP() throws UnknownHostException {
        // Arrange
        byte[] ip = {10, 2, 3, 1};
        InetAddress addr = InetAddress.getByAddress("testhost-1.mock", ip);
        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.mock", 2181));
        // myServer has a different hostname string so branch 1 is skipped
        InetSocketAddress myServer = new InetSocketAddress(
                InetAddress.getByAddress("alias.mock", ip), 2181);

        Map<String, InetAddress[]> dnsMap = new HashMap<>();
        dnsMap.put("testhost-1.mock", new InetAddress[]{addr});

        assertTrue(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(dnsMap), connectAddresses, myServer));
    }

    /**
     * Regression test for NPE fix: when the resolver throws UnknownHostException for the connect
     * hostname, resolvedConnectAddresses is null; iterating it must not throw NullPointerException.
     */
    @Test
    public void testIsMyServerInNewConfig_UnknownHostForConnectAddress_NoNPE() throws UnknownHostException {
        // Arrange
        byte[] ip = {10, 2, 3, 1};
        InetAddress myServerIP = InetAddress.getByAddress("testhost-1.mock", ip);
        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("unresolvable-connect.mock", 2181));
        // myServer is resolved so the (previously broken) branch would have been entered
        InetSocketAddress myServer = new InetSocketAddress(myServerIP, 2181);

        // resolver throws UnknownHostException for everything → resolvedConnectAddresses stays null
        assertFalse(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(Collections.emptyMap()), connectAddresses, myServer));
    }

    /**
     * Cross-matched resolved IPs (branch 4): myServer is unresolved (skipping the direct IP check),
     * but both the connect hostname and myServer's hostname resolve to overlapping IPs.
     * Models the case where the same server is reachable via two different DNS names
     * (e.g. testhost-1.mock and testhost-1.othermock, as set up in setupDNSMocks).
     */
    @Test
    public void testIsMyServerInNewConfig_CrossMatchedResolvedIPs() throws UnknownHostException {
        // Arrange – both names share the same IPs (mirrors setupDNSMocks for testhost-1.*)
        byte[] ip1 = {10, 2, 3, 1};
        byte[] ip2 = {10, 50, 50, 1};
        InetAddress addr1 = InetAddress.getByAddress("testhost-1.mock", ip1);
        InetAddress addr2 = InetAddress.getByAddress("testhost-1.mock", ip2);

        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.othermock", 2181));
        // myServer is unresolved → the direct IP comparison (branch 3) is skipped
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-1.mock", 2181);

        Map<String, InetAddress[]> dnsMap = new HashMap<>();
        dnsMap.put("testhost-1.othermock", new InetAddress[]{addr1, addr2});
        dnsMap.put("testhost-1.mock", new InetAddress[]{addr1, addr2});

        assertTrue(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(dnsMap), connectAddresses, myServer));
    }

    /**
     * Reverse-DNS match (branch 5): a resolved IP of myServer's hostname carries the connect
     * hostname as its own hostname — simulating a successful reverse-DNS lookup.
     */
    @Test
    public void testIsMyServerInNewConfig_ReverseDNSMatch() throws UnknownHostException {
        // Arrange
        byte[] ip = {10, 2, 3, 1};
        // InetAddress.getByAddress(host, bytes) sets getHostName() to the given host string,
        // which lets us simulate a reverse-DNS result without real network calls.
        InetAddress reverseDNSResult = InetAddress.getByAddress("testhost-1.mock", ip);

        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.mock", 2181));
        // myServer uses a different hostname so branches 1–4 all fail
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("my-server.zk", 2181);

        Map<String, InetAddress[]> dnsMap = new HashMap<>();
        // connect address resolution fails (no entry) → resolvedConnectAddresses = null
        // myServer resolution returns an address whose getHostName() == "testhost-1.mock"
        dnsMap.put("my-server.zk", new InetAddress[]{reverseDNSResult});

        assertTrue(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(dnsMap), connectAddresses, myServer));
    }

    /**
     * No match: myServer is genuinely absent from the connect list, all resolution strategies fail.
     */
    @Test
    public void testIsMyServerInNewConfig_NoMatch() throws UnknownHostException {
        // Arrange
        byte[] ip1 = {10, 2, 3, 1};
        byte[] ip9 = {10, 2, 3, 9};
        List<InetSocketAddress> connectAddresses = Collections.singletonList(
                InetSocketAddress.createUnresolved("testhost-1.mock", 2181));
        InetSocketAddress myServer = InetSocketAddress.createUnresolved("testhost-9.mock", 2181);

        Map<String, InetAddress[]> dnsMap = new HashMap<>();
        dnsMap.put("testhost-1.mock", new InetAddress[]{InetAddress.getByAddress("testhost-1.mock", ip1)});
        dnsMap.put("testhost-9.mock", new InetAddress[]{InetAddress.getByAddress("testhost-9.mock", ip9)});

        assertFalse(CustomZooKeeperHostProvider.isMyServerInNewConfig(
                mapResolver(dnsMap), connectAddresses, myServer));
    }

    private class TestResolver implements StaticHostProvider.Resolver {

        private byte counter = 1;

        @Override
        public InetAddress[] getAllByName(String name) throws UnknownHostException {
            if (name.contains("resolvable")) {
                byte[] addr = new byte[]{10, 0, 0, (byte) (counter++ % 10)};
                return new InetAddress[]{InetAddress.getByAddress(name, addr)};
            }
            throw new UnknownHostException();
        }

    }

    private double lowerboundCPS(int numClients, int numServers) {
        return (1 - slackPercent / 100.0) * numClients / numServers;
    }

    private double upperboundCPS(int numClients, int numServers) {
        return (1 + slackPercent / 100.0) * numClients / numServers;
    }

    /* DNS resolution tests */

    @Test
    public void testLiteralIPNoReverseNS() {
        byte size = 30;
        HostProvider hostProvider = getHostProviderUnresolved(size);
        for (int i = 0; i < size; i++) {
            InetSocketAddress next = hostProvider.next(0);
            assertThat(next, instanceOf(InetSocketAddress.class));
            assertFalse(next.isUnresolved());
            assertTrue(next.toString().startsWith("/"));
            // Do NOT trigger the reverse name service lookup.
            String hostname = next.getHostString();
            // In this case, the hostname equals literal IP address.
            assertEquals(next.getAddress().getHostAddress(), hostname);
        }
    }

    @Test
    public void testReResolvingSingle() throws UnknownHostException {
        // Arrange
        byte size = 1;
        ArrayList<InetSocketAddress> list = new ArrayList<>(size);

        // Test a hostname that resolves to a single address
        list.add(InetSocketAddress.createUnresolved("issues.apache.org", 1234));

        final InetAddress issuesApacheOrg = InetAddress.getByName("site1.mock");

        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                return new InetAddress[]{issuesApacheOrg};
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);

        // Act
        CustomZooKeeperHostProvider hostProvider = new CustomZooKeeperHostProvider(list, spyResolver);
        for (int i = 0; i < 10; i++) {
            InetSocketAddress next = hostProvider.next(0);
            assertEquals(issuesApacheOrg, next.getAddress());
        }

        // Assert
        // Resolver called 10 times, because we shouldn't cache the resolved addresses
        verify(spyResolver, times(10)).getAllByName("issues.apache.org"); // resolution occurred
    }

    @Test
    public void testReResolvingMultiple() throws UnknownHostException {
        // Arrange
        byte size = 1;
        ArrayList<InetSocketAddress> list = new ArrayList<>(size);

        // Test a hostname that resolves to multiple addresses
        list.add(InetSocketAddress.createUnresolved("www.apache.org", 1234));

        final InetAddress apacheOrg1 = InetAddress.getByName("site1.mock");

        final InetAddress apacheOrg2 = InetAddress.getByName("site2.mock");

        final List<InetAddress> resolvedAddresses = new ArrayList<>();
        resolvedAddresses.add(apacheOrg1);
        resolvedAddresses.add(apacheOrg2);
        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                return resolvedAddresses.toArray(new InetAddress[resolvedAddresses.size()]);
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);

        // Act & Assert
        CustomZooKeeperHostProvider hostProvider = new CustomZooKeeperHostProvider(list, spyResolver);
        assertEquals(1, hostProvider.size()); // single address not extracted

        for (int i = 0; i < 10; i++) {
            InetSocketAddress next = hostProvider.next(0);
            assertThat("Bad IP address returned", next.getAddress().getHostAddress(),
                    anyOf(equalTo(apacheOrg1.getHostAddress()), equalTo(apacheOrg2.getHostAddress())));
            assertEquals(1, hostProvider.size()); // resolve() call keeps the size of provider
        }
        // Resolver called 10 times, because we shouldn't cache the resolved addresses
        verify(spyResolver, times(10)).getAllByName("www.apache.org"); // resolution occurred
    }

    @Test
    public void testReResolveMultipleOneFailing() throws UnknownHostException {
        // Arrange
        final List<InetSocketAddress> list = new ArrayList<>();
        list.add(InetSocketAddress.createUnresolved("www.apache.org", 1234));
        final List<String> ipList = new ArrayList<>();
        final List<InetAddress> resolvedAddresses = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ipList.add(String.format("192.168.1.%d", i + 1));
            final InetAddress apacheOrg = InetAddress.getByName("site" + (i + 1) + ".mock");
            resolvedAddresses.add(apacheOrg);
        }

        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                return resolvedAddresses.toArray(new InetAddress[resolvedAddresses.size()]);
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);
        CustomZooKeeperHostProvider hostProvider = new CustomZooKeeperHostProvider(list, spyResolver);

        // Act & Assert
        InetSocketAddress resolvedFirst = hostProvider.next(0);
        assertFalse(resolvedFirst.isUnresolved(), "HostProvider should return resolved addresses");
        assertThat("Bad IP address returned", ipList, hasItems(resolvedFirst.getAddress().getHostAddress()));

        hostProvider.onConnected(); // first address worked

        InetSocketAddress resolvedSecond = hostProvider.next(0);
        assertFalse(resolvedSecond.isUnresolved(), "HostProvider should return resolved addresses");
        assertThat("Bad IP address returned", ipList, hasItems(resolvedSecond.getAddress().getHostAddress()));

        // Second address doesn't work, so we don't call onConnected() this time
        // CustomZooKeeperHostProvider should try to re-resolve the address in this case

        InetSocketAddress resolvedThird = hostProvider.next(0);
        assertFalse(resolvedThird.isUnresolved(), "HostProvider should return resolved addresses");
        assertThat("Bad IP address returned", ipList, hasItems(resolvedThird.getAddress().getHostAddress()));

        verify(spyResolver, times(3)).getAllByName("www.apache.org");  // resolution occurred every time
    }

    @Test
    public void testEmptyResolution() throws UnknownHostException {
        // Arrange
        final List<InetSocketAddress> list = new ArrayList<>();
        list.add(InetSocketAddress.createUnresolved("www.apache.org", 1234));
        list.add(InetSocketAddress.createUnresolved("www.google.com", 1234));
        final List<InetAddress> resolvedAddresses = new ArrayList<>();

        final InetAddress apacheOrg1 = InetAddress.getByName("site1.mock");

        resolvedAddresses.add(apacheOrg1);

        StaticHostProvider.Resolver resolver = new StaticHostProvider.Resolver() {
            @Override
            public InetAddress[] getAllByName(String name) {
                if ("www.apache.org".equalsIgnoreCase(name)) {
                    return resolvedAddresses.toArray(new InetAddress[resolvedAddresses.size()]);
                } else {
                    return new InetAddress[0];
                }
            }
        };
        StaticHostProvider.Resolver spyResolver = spy(resolver);
        CustomZooKeeperHostProvider hostProvider = new CustomZooKeeperHostProvider(list, spyResolver);

        // Act & Assert
        for (int i = 0; i < 10; i++) {
            InetSocketAddress resolved = hostProvider.next(0);
            hostProvider.onConnected();
            if (resolved.getHostName().equals("www.google.com")) {
                assertTrue(resolved.isUnresolved(),
                        "HostProvider should return unresolved address if host is unresolvable");
            } else {
                assertFalse(resolved.isUnresolved(), "HostProvider should return resolved addresses");
                assertEquals("192.168.1.1", resolved.getAddress().getHostAddress());
            }
        }

        verify(spyResolver, times(5)).getAllByName("www.apache.org");
        verify(spyResolver, times(5)).getAllByName("www.google.com");
    }

    @Test
    public void testReResolvingLocalhost() {
        byte size = 2;
        ArrayList<InetSocketAddress> list = new ArrayList<>(size);

        // Test a hostname that resolves to multiple addresses
        list.add(InetSocketAddress.createUnresolved("localhost", 1234));
        list.add(InetSocketAddress.createUnresolved("localhost", 1235));
        CustomZooKeeperHostProvider hostProvider = new CustomZooKeeperHostProvider(list);
        int sizeBefore = hostProvider.size();
        InetSocketAddress next = hostProvider.next(0);
        next = hostProvider.next(0);
        assertTrue(hostProvider.size() == sizeBefore,
                "Different number of addresses in the list: "
                        + hostProvider.size() + " (after), " + sizeBefore + " (before)");
    }

    private CustomZooKeeperHostProvider getHostProviderUnresolved(byte size) {
        return new CustomZooKeeperHostProvider(getUnresolvedServerAddresses(size), r.nextLong());
    }

    private Collection<InetSocketAddress> getUnresolvedServerAddresses(byte size) {
        ArrayList<InetSocketAddress> list = new ArrayList<>(size);
        while (size > 0) {
            list.add(InetSocketAddress.createUnresolved("10.10.10." + size, 1234 + size));
            --size;
        }
        return list;
    }

    private CustomZooKeeperHostProvider getHostProviderWithUnresolvedHostnames(int size) {
        return new CustomZooKeeperHostProvider(getUnresolvedHostnames(size), r.nextLong());
    }

    private Collection<InetSocketAddress> getUnresolvedHostnames(int size) {
        ArrayList<InetSocketAddress> list = new ArrayList<>(size);
        while (size > 0) {
            list.add(InetSocketAddress.createUnresolved(String.format("testhost-%d.mock", size), 1234
                    + size));
            --size;
        }
        System.out.println(Arrays.toString(list.toArray()));
        return list;
    }

    private static class MultiMappedHostResolver implements HostResolver {
        // hostname -> ordered list of pre-resolved InetAddress objects
        protected Map<String, List<InetAddress>> hostAliases;

        public MultiMappedHostResolver() {
            this(Collections.emptyMap());
        }

        /**
         * Accepts a map of hostname -> list of IP strings.
         * IPs are pre-resolved at construction time via NetUtil.
         */
        public MultiMappedHostResolver(Map<String, List<String>> hostAliases) {
            Map<String, List<InetAddress>> resolved = new LinkedHashMap<>();
            for (Map.Entry<String, List<String>> entry : hostAliases.entrySet()) {
                String hostName = entry.getKey();
                List<InetAddress> addresses = new ArrayList<>();
                for (String ip : entry.getValue()) {
                    InetAddress addr = createAddress(hostName, ip);
                    if (addr != null) {
                        addresses.add(addr);
                    }
                }
                if (!addresses.isEmpty()) {
                    resolved.put(hostName, addresses);
                }
            }
            this.hostAliases = resolved;
        }

        private static InetAddress createAddress(String hostName, String iPAddress) {
            InetAddress addr = NetUtil.createInetAddressFromIpAddressString(iPAddress);
            if (addr == null) {
                return null;
            }
            try {
                // Re-create with the hostname label attached (mirrors MappedHostResolver behaviour)
                return InetAddress.getByAddress(hostName, addr.getAddress());
            } catch (UnknownHostException e) {
                return null;
            }
        }

        @Override
        public Collection<InetAddress> getAllAddressesForHostName(Map<String, Object> argumentMap) {
            String hostName = (String) getMethodArguments(argumentMap)[0];
            List<InetAddress> addresses = hostAliases.get(hostName);
            // Return a copy so callers cannot mutate internal state
            return addresses != null ? new ArrayList<>(addresses) : new ArrayList<>();
        }

        @Override
        public Collection<String> getAllHostNamesForHostAddress(Map<String, Object> argumentMap) {
            byte[] address = (byte[]) getMethodArguments(argumentMap)[0];
            String iPAddress = IPAddressUtil.INSTANCE.numericToTextFormat(address);
            Collection<String> hostNames = new ArrayList<>();
            for (Map.Entry<String, List<InetAddress>> entry : hostAliases.entrySet()) {
                for (InetAddress addr : entry.getValue()) {
                    if (IPAddressUtil.INSTANCE.numericToTextFormat(addr.getAddress()).equals(iPAddress)) {
                        hostNames.add(entry.getKey());
                        break;
                    }
                }
            }
            return hostNames;
        }

        public synchronized MultiMappedHostResolver putHost(String hostname, String... iPs) {
            Map<String, List<InetAddress>> hostAliases = new LinkedHashMap<>(this.hostAliases);
            List<InetAddress> addresses = new ArrayList<>();
            for (String ip : iPs) {
                InetAddress addr = createAddress(hostname, ip);
                if (addr != null) {
                    addresses.add(addr);
                }
            }
            if (!addresses.isEmpty()) {
                hostAliases.put(hostname, addresses);
            }
            this.hostAliases = hostAliases;
            return this;
        }

        public synchronized MultiMappedHostResolver removeHost(String hostname) {
            Map<String, List<InetAddress>> hostAliases = new LinkedHashMap<>(this.hostAliases);
            hostAliases.remove(hostname);
            this.hostAliases = hostAliases;
            return this;
        }

        public synchronized MultiMappedHostResolver removeHostForIP(String iP) {
            Map<String, List<InetAddress>> hostAliases = new LinkedHashMap<>(this.hostAliases);
            hostAliases.entrySet().removeIf(entry ->
                    entry.getValue().stream()
                            .anyMatch(addr -> IPAddressUtil.INSTANCE.numericToTextFormat(addr.getAddress()).equals(iP))
            );
            this.hostAliases = hostAliases;
            return this;
        }

        @Override
        public boolean isReady(HostResolutionRequestInterceptor hostResolverService) {
            return HostResolver.super.isReady(hostResolverService) && obtainsResponseForMappedHost();
        }

        protected synchronized boolean obtainsResponseForMappedHost() {
            String hostNameForTest = null;
            if (hostAliases.isEmpty()) {
                putHost(hostNameForTest = UUID.randomUUID().toString(), "127.0.0.1");
            }
            try {
                for (String hostname : hostAliases.keySet()) {
                    InetAddress.getByName(hostname);
                }
                return true;
            } catch (UnknownHostException exc) {
                return false;
            } finally {
                if (hostNameForTest != null) {
                    removeHost(hostNameForTest);
                }
            }
        }
    }
}
