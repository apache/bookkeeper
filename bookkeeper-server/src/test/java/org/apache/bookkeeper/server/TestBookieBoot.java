/*
 *
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
 *
 */
package org.apache.bookkeeper.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.File;
import java.io.FileWriter;
import java.net.Socket;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.api.WriteHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that a bookie can boot via the main method
 * and serve read and write requests.
 */
public class TestBookieBoot extends BookKeeperClusterTestCase {
    private static final Logger log = LoggerFactory.getLogger(TestBookieBoot.class);
    public TestBookieBoot() throws Exception {
        super(0);
    }

    @Test
    public void testBootFromConfig() throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setMetadataServiceUri(this.metadataServiceUri);
        conf.setAllowLoopback(true);
        conf.setBookiePort(PortManager.nextFreePort());
        conf.setLedgerStorageClass("org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage");

        File storageDir = tmpDirs.createNew("bookie", "storage");
        conf.setLedgerDirNames(new String[] { storageDir.toString() });
        conf.setJournalDirName(storageDir.toString());

        PropertiesConfiguration propsConf = new PropertiesConfiguration();
        for (Iterator<String> iter = conf.getKeys(); iter.hasNext(); ) {
            String key = iter.next();
            propsConf.setProperty(key, conf.getProperty(key));
        }

        File confFile = File.createTempFile("test", "conf");
        try (FileWriter writer = new FileWriter(confFile)) {
            propsConf.write(writer);
        }

        log.info("Conf: {}", confFile);

        CompletableFuture<Integer> promise = new CompletableFuture<>();
        Thread t = new Thread(() -> {
            try {
                int ret = Main.doMain(new String[] {"-c", confFile.toString()});
                promise.complete(ret);
            } catch (Exception e) {
                promise.completeExceptionally(e);
            }
        }, "bookie-main");
        t.start();

        BookieSocketAddress addr = BookieImpl.getBookieAddress(conf);
        BookKeeperTestClient bkc = new BookKeeperTestClient(baseClientConf);
        bkc.waitForWritableBookie(addr.toBookieId()).get();

        boolean connected = false;
        for (int i = 0; i < 100 && t.isAlive(); i++) {
            try (Socket s = new Socket(addr.getSocketAddress().getAddress(), addr.getPort())) {
                connected = true;
                break;
            } catch (Exception e) {
                // expected, will retry
            }
            Thread.sleep(100);
        }
        assertThat(connected, equalTo(true));

        long ledgerId;
        try (WriteHandle wh = bkc.newCreateLedgerOp().withEnsembleSize(1)
                .withWriteQuorumSize(1).withAckQuorumSize(1)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .execute().get()) {
            ledgerId = wh.getId();
            wh.append("foobar".getBytes(UTF_8));
        }

        try (ReadHandle rh = bkc.newOpenLedgerOp().withLedgerId(ledgerId)
                .withDigestType(DigestType.CRC32C)
                .withPassword(new byte[0])
                .withRecovery(true)
                .execute().get()) {
            assertThat(rh.getLastAddConfirmed(), equalTo(0L));
            try (LedgerEntries entries = rh.read(0, 0)) {
                assertThat(new String(entries.getEntry(0).getEntryBytes(), UTF_8), equalTo("foobar"));
            }
        }

        t.interrupt();
        assertThat(promise.get(10, TimeUnit.SECONDS), equalTo(ExitCode.OK));
    }
}
