/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.grpc.resolver;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.junit.Assert.assertEquals;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolver.Listener;
import io.grpc.Status;
import io.grpc.SynchronizationContext;
import io.grpc.internal.GrpcUtil;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test {@link BKRegistrationNameResolver}.
 */
public class BKRegistrationNameResolverTest extends BookKeeperClusterTestCase {

    private static final String ROOT_PATH = "/resolver-test";
    private static final String SERVERS_PATH = ROOT_PATH + "/servers";

    private final BKRegistrationNameResolverProvider resolverProvider;

    private MetadataBookieDriver bookieDriver;
    private RegistrationManager regManager;
    private URI serviceUri;

    public BKRegistrationNameResolverTest() {
        super(0);
        this.resolverProvider = new BKRegistrationNameResolverProvider();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        zkc.transaction()
            .create(ROOT_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
            .create(SERVERS_PATH, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
            .create(SERVERS_PATH + "/" + AVAILABLE_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
            .commit();

        serviceUri = URI.create("zk://" + zkUtil.getZooKeeperConnectString() + SERVERS_PATH);


        ServerConfiguration serverConf = new ServerConfiguration();
        serverConf.setMetadataServiceUri(serviceUri.toString());
        bookieDriver = MetadataDrivers.getBookieDriver(serviceUri);
        bookieDriver.initialize(serverConf, NullStatsLogger.INSTANCE);
        regManager = bookieDriver.createRegistrationManager();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        regManager.close();
        bookieDriver.close();

        super.tearDown();
    }

    @Test
    public void testNameResolver() throws Exception {
        int numServers = 3;

        Set<SocketAddress> addressSet = new HashSet<>();
        for (int i = 0; i < numServers; i++) {
            InetSocketAddress address = new InetSocketAddress("127.0.0.1", 3181 + i);
            addressSet.add(address);
            bookieDriver.createRegistrationManager().registerBookie(
                BookieId.parse("127.0.0.1:" + (3181 + i)), false, BookieServiceInfo.EMPTY
            );
        }

        LinkedBlockingQueue<List<EquivalentAddressGroup>> notifications = new LinkedBlockingQueue<>();


        @Cleanup("shutdown")
        NameResolver resolver = resolverProvider.newNameResolver(serviceUri,
                NameResolver.Args.newBuilder()
                        .setDefaultPort(0)
                        .setProxyDetector(GrpcUtil.DEFAULT_PROXY_DETECTOR)
                        .setSynchronizationContext(new SynchronizationContext((t, ex) -> {}))
                        .setServiceConfigParser(new NameResolver.ServiceConfigParser() {
                            @Override
                            public NameResolver.ConfigOrError parseServiceConfig(Map<String, ?> rawServiceConfig) {
                                return null;
                            }
                        })
                        .build());
        resolver.start(new Listener() {
            @Override
            public void onAddresses(List<EquivalentAddressGroup> servers, Attributes attributes) {
                notifications.add(servers);
            }

            @Override
            public void onError(Status error) {

            }
        });

        List<EquivalentAddressGroup> groups = notifications.take();
        assertEquals(numServers, groups.size());

        Set<SocketAddress> receivedSet = groups.stream()
            .map(group -> group.getAddresses().get(0))
            .collect(Collectors.toSet());
        assertEquals(addressSet, receivedSet);

        // add 3 more servers

        for (int i = numServers; i < 2 * numServers; i++) {
            InetSocketAddress address = new InetSocketAddress("127.0.0.1", 3181 + i);
            addressSet.add(address);
            regManager.registerBookie(
                BookieId.parse("127.0.0.1:" + (3181 + i)), false, BookieServiceInfo.EMPTY
            );
        }

        List<EquivalentAddressGroup> notification = notifications.take();
        while (notification.size() < 2 * numServers) {
            notification = notifications.take();
        }
        assertEquals(2 * numServers, notification.size());
        receivedSet = notification.stream()
            .map(group -> group.getAddresses().get(0))
            .collect(Collectors.toSet());
        assertEquals(addressSet, receivedSet);
    }

}
