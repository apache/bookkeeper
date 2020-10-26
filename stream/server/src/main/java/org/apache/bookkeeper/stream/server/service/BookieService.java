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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
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
    private final Supplier<BookieServiceInfo> bookieServiceInfoProvider;


    public BookieService(BookieConfiguration conf, StatsLogger statsLogger,
                         Supplier<BookieServiceInfo> bookieServiceInfoProvider) {
        super("bookie-server", conf, statsLogger);
        this.serverConf = new ServerConfiguration();
        this.serverConf.loadConf(conf.getUnderlyingConf());
        this.bookieServiceInfoProvider = bookieServiceInfoProvider;
    }

    @Override
    protected void doStart() {
        List<File> indexDirs;
        if (null == serverConf.getIndexDirs()) {
            indexDirs = Collections.emptyList();
        } else {
            indexDirs = Arrays.asList(serverConf.getIndexDirs());
        }
        log.info("Hello, I'm your bookie, listening on port {} :"
                + " metadata service uri = {}, journals = {}, ledgers = {}, index = {}",
            serverConf.getBookiePort(),
            serverConf.getMetadataServiceUriUnchecked(),
            Arrays.asList(serverConf.getJournalDirNames()),
            Arrays.asList(serverConf.getLedgerDirs()),
            indexDirs);
        try {
            this.bs = new BookieServer(serverConf, statsLogger, bookieServiceInfoProvider);
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
}
