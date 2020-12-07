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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stream.server.conf.BookieConfiguration;

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

    public BookieService(BookieConfiguration conf, StatsLogger statsLogger,
                         Supplier<BookieServiceInfo> bookieServiceInfoProvider) throws Exception {
        super("bookie-server", conf, statsLogger);
        this.serverConf = new ServerConfiguration();
        this.serverConf.loadConf(conf.getUnderlyingConf());
        String hello = String.format(
            "Hello, I'm your bookie, bookieId is %1$s, listening on port %2$s. Metadata service uri is %3$s."
                + " Journals are in %4$s. Ledgers are stored in %5$s.",
            serverConf.getBookieId() != null ? serverConf.getBookieId() : "<not-set>",
            serverConf.getBookiePort(),
            serverConf.getMetadataServiceUriUnchecked(),
            Arrays.asList(serverConf.getJournalDirNames()),
            Arrays.asList(serverConf.getLedgerDirNames()));
        log.info(hello);
        this.bs = new BookieServer(serverConf, statsLogger, bookieServiceInfoProvider);
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
