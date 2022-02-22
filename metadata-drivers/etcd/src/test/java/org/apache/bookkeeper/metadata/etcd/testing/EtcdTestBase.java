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

package org.apache.bookkeeper.metadata.etcd.testing;

import io.etcd.jetcd.Client;

import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * A test base that setup etcd cluster for testing.
 */
@Slf4j
public abstract class EtcdTestBase {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(120);

    protected static EtcdContainer etcdContainer;

    @BeforeClass
    public static void setupCluster() throws Exception {
        etcdContainer = new EtcdContainer(RandomStringUtils.randomAlphabetic(8));
        etcdContainer.start();
        log.info("Successfully started etcd at {}", etcdContainer.getClientEndpoint());
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        if (null != etcdContainer) {
            etcdContainer.stop();
            log.info("Successfully stopped etcd.");
        }
    }

    protected Client etcdClient;

    protected static Client newEtcdClient() {
        Client client = Client.builder()
            .endpoints(etcdContainer.getClientEndpoint())
            .build();
        return client;
    }

    protected static <T> Consumer<Versioned<Set<T>>> consumeVersionedKeySet(
        LinkedBlockingQueue<Versioned<Set<T>>> notifications) {
        return versionedKeys -> {
            log.info("Received new keyset : {}", versionedKeys);
            try {
                notifications.put(versionedKeys);
            } catch (InterruptedException e) {
                log.error("Interrupted at enqueuing updated key set", e);
            }
        };
    }

    @Before
    public void setUp() throws Exception {
        etcdClient = newEtcdClient();
        log.info("Successfully build etcd client to endpoint {}", etcdContainer.getClientEndpoint());
    }

    @After
    public void tearDown() throws Exception {
        if (null != etcdClient) {
            etcdClient.close();
            log.info("Successfully close etcd client to endpoint {}", etcdContainer.getClientEndpoint());
        }
    }

}
