/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import javax.security.auth.login.Configuration;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test basic functions using secured ZooKeeper.
 */
public class EnableZkSecurityBasicTest extends BookKeeperClusterTestCase {

    public EnableZkSecurityBasicTest() {
        super(0);
        this.baseClientConf.setZkEnableSecurity(true);
        this.baseConf.setZkEnableSecurity(true);
    }

    @BeforeClass
    public static void setupJAAS() throws IOException {
        System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        File tmpJaasDir = Files.createTempDirectory("jassTmpDir").toFile();
        File tmpJaasFile = new File(tmpJaasDir, "jaas.conf");
        String jassFileContent = "Server {\n"
            + "       org.apache.zookeeper.server.auth.DigestLoginModule required\n"
            + "       user_foo=\"bar\";\n"
            + "};\n"
            + "\n"
            + "Client {\n"
            + "       org.apache.zookeeper.server.auth.DigestLoginModule required\n"
            + "       username=\"foo\"\n"
            + "       password=\"bar\";\n"
            + "};";
        Files.write(tmpJaasFile.toPath(), jassFileContent.getBytes(StandardCharsets.UTF_8));
        System.setProperty("java.security.auth.login.config", tmpJaasFile.getAbsolutePath());
        Configuration.getConfiguration().refresh();
    }

    @AfterClass
    public static void cleanUpJAAS() {
        System.clearProperty("java.security.auth.login.config");
        Configuration.getConfiguration().refresh();
        System.clearProperty("zookeeper.authProvider.1");
    }

    @Test
    public void testCreateLedgerAddEntryOnSecureZooKeepeer() throws Exception {
        startNewBookie();

        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri(zkUtil.getMetadataServiceUri());
        conf.setZkTimeout(20000);

        conf.setZkEnableSecurity(true);

        try (BookKeeper bkc = new BookKeeper(conf)) {
            try (LedgerHandle lh = bkc.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, "testPasswd".getBytes())) {
                lh.addEntry("foo".getBytes(StandardCharsets.UTF_8));
            }
        }

        checkAllAcls();
    }

    private void checkAllAcls() throws IOException, InterruptedException, KeeperException {
        ZooKeeper zk = ZooKeeperClient.newBuilder()
            .connectString(zkUtil.getZooKeeperConnectString())
            .sessionTimeoutMs(20000)
            .build();
        checkACls(zk, "/");
        zk.close();
    }

    private void checkACls(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        List<String> children = zk.getChildren(path, null);
        for (String child : children) {
            if (child.equals(READONLY)) {
                continue;
            }

            String fullPath = path.equals("/") ? path + child : path + "/" + child;
            List<ACL> acls = zk.getACL(fullPath, new Stat());
            checkACls(zk, fullPath);

            if (!fullPath.startsWith("/zookeeper") // skip zookeeper internal nodes
                && !fullPath.equals("/ledgers") // node created by test setup
                && !fullPath.equals("/ledgers/" + BookKeeperConstants.AVAILABLE_NODE)
                && !fullPath.equals("/ledgers/" + BookKeeperConstants.INSTANCEID) // node created by test setup
                ) {
                assertEquals(1, acls.size());
                assertEquals(31, acls.get(0).getPerms());
                assertEquals(31, acls.get(0).getPerms());
                assertEquals("unexpected ACLS on " + fullPath + ": " + acls.get(0), "foo", acls.get(0).getId().getId());
                assertEquals("unexpected ACLS on " + fullPath + ": " + acls.get(0), "sasl",
                        acls.get(0).getId().getScheme());
            }
        }
    }
}
