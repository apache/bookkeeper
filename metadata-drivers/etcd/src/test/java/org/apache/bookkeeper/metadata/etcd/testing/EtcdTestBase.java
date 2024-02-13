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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

/**
 * A test base that setup etcd cluster for testing.
 */
@Slf4j
public abstract class EtcdTestBase {

    @Rule
    public Timeout globalTimeout = Timeout.seconds(120);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static EtcdContainer etcdContainer;

    @BeforeClass
    public static void setupCluster() throws Exception {
        if (null == etcdContainer) {
            etcdContainer = new EtcdContainer(RandomStringUtils.randomAlphabetic(8), false);
            etcdContainer.start();
            log.info("Successfully started etcd at {}", etcdContainer.getClientEndpoint());
        }
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        if (null != etcdContainer) {
            etcdContainer.stop();
            etcdContainer = null;
            log.info("Successfully stopped etcd.");
        }
    }

    protected Client etcdClient;

    @SneakyThrows
    protected static Client newEtcdClient() {
        Client client = Client.builder()
            .endpoints(etcdContainer.getClientEndpoint())
            .sslContext(etcdContainer.getSslContext())
            .authority(etcdContainer.getAuthority())
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

    @SneakyThrows
    protected static String getMetadataServiceConfig() {
        if (!etcdContainer.isSecure()) {
            return "";
        }

        Path config = Files.createTempFile("etcd", "conf");
        String contents = "useTls=true"
            + "\ntlsProvider=OPENSSL"
            + "\ntlsTrustCertsFilePath="
            + unpackSslResource("ca.pem").toString()
            + "\ntlsKeyFilePath="
            + unpackSslResource("client-key-pk8.pem").toString()
            + "\ntlsCertificateFilePath="
            + unpackSslResource("client.pem").toString()
            + "\nauthority="
            + etcdContainer.getAuthority();
        Files.write(config, contents.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
        return config.toString();
    }

    @SneakyThrows
    protected static Path unpackSslResource(String name) {
        @Cleanup
        InputStream resource = EtcdTestBase.class.getClassLoader().getResourceAsStream("ssl/cert/" + name);
        Path target = Files.createTempFile("bk", name);
        @Cleanup
        OutputStream out =
                Files.newOutputStream(target, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        IOUtils.copy(resource, out);
        return target;
    }
}
