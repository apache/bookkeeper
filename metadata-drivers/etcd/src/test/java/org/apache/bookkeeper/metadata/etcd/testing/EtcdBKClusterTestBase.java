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

package org.apache.bookkeeper.metadata.etcd.testing;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.MetadataDrivers;
import org.apache.bookkeeper.metadata.etcd.EtcdMetadataBookieDriver;
import org.apache.bookkeeper.metadata.etcd.EtcdMetadataClientDriver;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * A test base that run an Etcd based bookkeeper cluster.
 */
@Slf4j
public abstract class EtcdBKClusterTestBase extends EtcdTestBase {

    protected static ClientConfiguration baseClientConf;
    protected static ServerConfiguration baseServerConf;
    protected static final int NUM_BOOKIES = 3;
    protected static final List<BookieServer> BOOKIES = new ArrayList<>(NUM_BOOKIES);
    protected static final List<File> TMP_DIRS = new ArrayList<>(NUM_BOOKIES);

    protected static File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        TMP_DIRS.add(dir);
        return dir;
    }

    protected static ServerConfiguration newServerConfiguration() throws Exception {
        File f = createTempDir("bookie", "test");
        int port = PortManager.nextFreePort();
        return newServerConfiguration(port, f, new File[] { f });
    }

    protected static ServerConfiguration newServerConfiguration(int port, File journalDir, File[] ledgerDirs) {
        ServerConfiguration conf = new ServerConfiguration(baseServerConf);
        conf.setBookiePort(port);
        conf.setJournalDirName(journalDir.getPath());
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i = 0; i < ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        conf.setEnableTaskExecutionStats(true);
        return conf;
    }

    @BeforeClass
    public static void setupCluster() throws Exception {
        setupCluster(NUM_BOOKIES);
    }
    protected static void setupCluster(int numBookies) throws Exception {
        EtcdTestBase.setupCluster();

        MetadataDrivers.registerBookieDriver(
            "etcd", EtcdMetadataBookieDriver.class
        );
        MetadataDrivers.registerClientDriver(
            "etcd", EtcdMetadataClientDriver.class
        );

        log.info("Successfully started etcd at:"
                + " internal service uri = {}, external service uri = {}",
            etcdContainer.getInternalServiceUri(), etcdContainer.getExternalServiceUri());

        ServiceURI uri = ServiceURI.create(etcdContainer.getExternalServiceUri());

        baseClientConf = new ClientConfiguration()
            .setMetadataServiceUri(uri.getUri().toString());
        baseServerConf = TestBKConfiguration.newServerConfiguration()
            .setMetadataServiceUri(uri.getUri().toString());
        // format the cluster
        assertTrue(BookKeeperAdmin.format(baseServerConf, false, true));
        // start bookies
        startNumBookies(numBookies);
    }

    private static void startNumBookies(int numBookies) throws Exception {
        for (int i = 0; i < numBookies; i++) {
            ServerConfiguration conf = newServerConfiguration();
            log.info("Starting new bookie on port : {}", conf.getBookiePort());
            BookieServer server = startBookie(conf);
            synchronized (BOOKIES) {
                BOOKIES.add(server);
            }
        }
    }
    private static BookieServer startBookie(ServerConfiguration conf) throws Exception {
        conf.setAutoRecoveryDaemonEnabled(true);
        TestStatsProvider provider = new TestStatsProvider();
        BookieServer server = new BookieServer(conf, provider.getStatsLogger(""));
        server.start();
        return server;
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        // stop bookies
        stopBookies();
        // stop metadata store
        EtcdTestBase.teardownCluster();
        log.info("Stopped the metadata store.");
        // clean up temp dirs
        for (File f : TMP_DIRS) {
            FileUtils.deleteDirectory(f);
        }
        log.info("Clean up all the temp directories.");
    }

    private static void stopBookies() {
        synchronized (BOOKIES) {
            BOOKIES.forEach(BookieServer::shutdown);
            log.info("Stopped all the bookies.");
        }
    }

    protected ClientConfiguration conf;
    protected BookKeeper bk;

    @Before
    public void setUp() throws Exception {
        conf = new ClientConfiguration()
            .setMetadataServiceUri(etcdContainer.getExternalServiceUri());
        bk = BookKeeper.newBuilder(conf).build();
    }

    @After
    public void tearDown() throws Exception {
        if (null != bk) {
            bk.close();
        }
    }

}
