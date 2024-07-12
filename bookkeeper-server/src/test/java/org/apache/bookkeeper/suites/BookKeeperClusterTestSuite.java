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

package org.apache.bookkeeper.suites;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import io.netty.buffer.PooledByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.MockUncleanShutdownDetection;
import org.apache.bookkeeper.bookie.TestBookieImpl;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.net.ServiceURI;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.TestStatsProvider;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * A class runs a bookkeeper cluster for testing.
 *
 * <p>The cluster will be setup and teardown before class. It will not be restarted between different
 * test methods. It is more suitable for running tests that don't require restarting bookies.
 */
@Slf4j
public abstract class BookKeeperClusterTestSuite {

    protected static MetadataStore metadataStore;
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
    public static void setUpCluster() throws Exception {
        setUpCluster(NUM_BOOKIES);
    }

    protected static void setUpCluster(int numBookies) throws Exception {
        // set up the metadata store
        metadataStore = new ZKMetadataStore();
        metadataStore.start();
        ServiceURI uri = metadataStore.getServiceUri();
        log.info("Setting up cluster at service uri : {}", uri.getUri());

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
        BookieServer server = new BookieServer( conf, new TestBookieImpl(conf),
                NullStatsLogger.INSTANCE, PooledByteBufAllocator.DEFAULT,
                new MockUncleanShutdownDetection());
        server.start();
        return server;
    }

    @AfterClass
    public static void tearDownCluster() throws Exception {
        // stop bookies
        stopBookies();

        // stop metadata store
        metadataStore.close();
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



}
