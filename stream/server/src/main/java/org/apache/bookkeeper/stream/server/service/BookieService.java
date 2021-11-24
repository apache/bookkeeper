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
package org.apache.bookkeeper.stream.server.service;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.BOOKIE_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_INDEX_SCOPE;
import static org.apache.bookkeeper.bookie.BookKeeperServerStats.LD_LEDGER_SCOPE;

import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.BookieResources;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.LegacyCookieValidation;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;
import org.apache.bookkeeper.util.DiskChecker;

/**
 * A {@link org.apache.bookkeeper.common.component.LifecycleComponent} that runs
 * a {@link org.apache.bookkeeper.proto.BookieServer}.
 */
@Accessors(fluent = true)
@Slf4j
public class BookieService extends AbstractLifecycleComponent<BookieConfiguration> {

    @Getter
    private final ServerConfiguration serverConf;
    private BookieServer bs;
    private MetadataBookieDriver metadataDriver;
    private RegistrationManager rm;
    private LedgerManagerFactory lmFactory;
    private LedgerManager ledgerManager;
    private Supplier<BookieServiceInfo> bookieServiceInfoProvider;

    public BookieService(BookieConfiguration conf, StatsLogger statsLogger,
                         Supplier<BookieServiceInfo> bookieServiceInfoProvider) throws Exception {
        super("bookie-server", conf, statsLogger);
        this.serverConf = new ServerConfiguration();
        this.serverConf.loadConf(conf.getUnderlyingConf());
        this.bookieServiceInfoProvider = bookieServiceInfoProvider;
        String hello = String.format(
            "Hello, I'm your bookie, bookieId is %1$s, listening on port %2$s. Metadata service uri is %3$s."
                + " Journals are in %4$s. Ledgers are stored in %5$s.",
            serverConf.getBookieId() != null ? serverConf.getBookieId() : "<not-set>",
            serverConf.getBookiePort(),
            serverConf.getMetadataServiceUriUnchecked(),
            Arrays.asList(serverConf.getJournalDirNames()),
            Arrays.asList(serverConf.getLedgerDirNames()));

        ByteBufAllocator allocator = BookieResources.createAllocator(serverConf);

        this.metadataDriver = BookieResources.createMetadataDriver(
                serverConf, statsLogger);
        StatsLogger bookieStats = statsLogger.scope(BOOKIE_SCOPE);
        this.rm = this.metadataDriver.createRegistrationManager();
        this.lmFactory = this.metadataDriver.getLedgerManagerFactory();
        this.ledgerManager = this.lmFactory.newLedgerManager();

        DiskChecker diskChecker = BookieResources.createDiskChecker(serverConf);
        LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(
                serverConf, diskChecker, bookieStats.scope(LD_LEDGER_SCOPE));
        LedgerDirsManager indexDirsManager = BookieResources.createIndexDirsManager(
                serverConf, diskChecker, bookieStats.scope(LD_INDEX_SCOPE), ledgerDirsManager);
        LedgerStorage storage = BookieResources.createLedgerStorage(
                serverConf, ledgerManager, ledgerDirsManager, indexDirsManager, bookieStats, allocator);

        LegacyCookieValidation cookieValidation = new LegacyCookieValidation(serverConf, rm);
        cookieValidation.checkCookies(Main.storageDirectoriesFromConf(serverConf));

        Bookie bookie;
        if (serverConf.isForceReadOnlyBookie()) {
            bookie = new ReadOnlyBookie(serverConf, rm, storage, diskChecker,
                    ledgerDirsManager, indexDirsManager,
                    statsLogger.scope(BOOKIE_SCOPE),
                    allocator, bookieServiceInfoProvider);
        } else {
            bookie = new BookieImpl(serverConf, rm, storage, diskChecker,
                    ledgerDirsManager, indexDirsManager,
                    statsLogger.scope(BOOKIE_SCOPE),
                    allocator, bookieServiceInfoProvider);
        }

        this.bs = new BookieServer(serverConf, bookie,
                statsLogger, allocator);

        log.info(hello);
    }

    @Override
    protected void doStart() {
        try {
            bs.start();
            log.info("Started bookie server successfully.");
        } catch (Exception e) {
            throw new RuntimeException("Failed to start bookie server", e);
        }
    }

    @Override
    protected void doStop() {
        if (null != bs) {
            bs.shutdown();
        }
        if (rm != null) {
            rm.close();
        }
        if (ledgerManager != null) {
            try {
                ledgerManager.close();
            } catch (Exception e) {
                log.error("Error shutting down ledger manager", e);
            }
        }
        if (lmFactory != null) {
            try {
                lmFactory.close();
            } catch (Exception e) {
                log.error("Error shutting down ledger manager factory", e);
            }
        }
        if (null != metadataDriver) {
            metadataDriver.close();
        }
    }

    @Override
    protected void doClose() throws IOException {
        // no-op
    }

    public BookieServer getServer() {
        return bs;
    }

    @Override
    public void publishInfo(ComponentInfoPublisher componentInfoPublisher) {
        try {
            BookieSocketAddress localAddress = bs.getLocalAddress();
            List<String> extensions = new ArrayList<>();
            if (serverConf.getTLSProviderFactoryClass() != null) {
                extensions.add("tls");
            }
            ComponentInfoPublisher.EndpointInfo endpoint = new ComponentInfoPublisher.EndpointInfo("bookie",
                    localAddress.getPort(),
                    localAddress.getHostName(),
                    "bookie-rpc", null, extensions);
            componentInfoPublisher.publishEndpoint(endpoint);

        } catch (UnknownHostException err) {
            log.error("Cannot compute local address", err);
        }
    }
}
