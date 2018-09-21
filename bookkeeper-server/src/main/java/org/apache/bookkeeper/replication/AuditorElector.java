/**
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
package org.apache.bookkeeper.replication;

import static org.apache.bookkeeper.replication.ReplicationStats.ELECTION_ATTEMPTS;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.concurrent.Future;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.AuditorSelector;
import org.apache.bookkeeper.meta.AuditorSelector.SelectorListener;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performing auditor election using Apache ZooKeeper. Using ZooKeeper as a
 * coordination service, when a bookie bids for auditor, it creates an ephemeral
 * sequential file (znode) on ZooKeeper and considered as their vote. Vote
 * format is 'V_sequencenumber'. Election will be done by comparing the
 * ephemeral sequential numbers and the bookie which has created the least znode
 * will be elected as Auditor. All the other bookies will be watching on their
 * predecessor znode according to the ephemeral sequence numbers.
 */
public class AuditorElector implements SelectorListener {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorElector.class);

    private final String bookieId;
    private final ServerConfiguration conf;
    private final BookKeeper bkc;
    private final boolean ownBkc;

    private final AuditorSelector selector;
    Auditor auditor;

    // Expose Stats
    private final Counter electionAttempts;
    private final StatsLogger statsLogger;

    @VisibleForTesting
    public AuditorElector(final String bookieId, ServerConfiguration conf) throws UnavailableException {
        this(
            bookieId,
            conf,
            Auditor.createBookKeeperClientThrowUnavailableException(conf),
            true);
    }

    /**
     * AuditorElector for performing the auditor election.
     *
     * @param bookieId
     *            - bookie identifier, comprises HostAddress:Port
     * @param conf
     *            - configuration
     * @param bkc
     *            - bookkeeper instance
     * @throws UnavailableException
     *             throws unavailable exception while initializing the elector
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          boolean ownBkc) throws UnavailableException {
        this(bookieId, conf, bkc, NullStatsLogger.INSTANCE, ownBkc);
    }

    /**
     * AuditorElector for performing the auditor election.
     *
     * @param bookieId
     *            - bookie identifier, comprises HostAddress:Port
     * @param conf
     *            - configuration
     * @param bkc
     *            - bookkeeper instance
     * @param statsLogger
     *            - stats logger
     * @throws UnavailableException
     *             throws unavailable exception while initializing the elector
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          StatsLogger statsLogger,
                          boolean ownBkc) throws UnavailableException {
        this.bookieId = bookieId;
        this.conf = conf;
        this.bkc = bkc;
        this.ownBkc = ownBkc;
        this.statsLogger = statsLogger;
        this.electionAttempts = statsLogger.getCounter(ELECTION_ATTEMPTS);
        this.selector = bkc.getMetadataClientDriver().getAuditorSelector(bookieId);
    }

    public Future<?> start() {
        try {
            return selector.select(this);
        } catch (MetadataException e) {
            return FutureUtils.exception(e);
        }
    }

    @VisibleForTesting
    Auditor getAuditor() {
        return auditor;
    }

    /**
     * Shutting down AuditorElector.
     */
    public void shutdown() throws InterruptedException {
        if (selector != null) {
            selector.close();
        }
        if (auditor != null) {
            auditor.shutdown();
            auditor = null;
        }
        if (ownBkc) {
            try {
                bkc.close();
            } catch (BKException e) {
                LOG.warn("Failed to close bookkeeper client", e);
            }
        }
    }

    /**
     * If current bookie is running as auditor, return the status of the
     * auditor. Otherwise return the status of elector.
     *
     * @return
     */
    public boolean isRunning() {
        if (auditor != null) {
            return auditor.isRunning();
        }
        return selector.isRunning();
    }

    @Override
    public String toString() {
        return "AuditorElector for " + bookieId;
    }

    @Override
    public void onLeaderSelected() throws IOException {
        try {
            auditor = new Auditor(bookieId, conf, bkc, false, statsLogger);
            auditor.start();
        } catch (UnavailableException e) {
            throw new IOException("Failed to create auditor after it is selected as auditor leader", e);
        }
    }

    @Override
    public void onSelectionAttempt() {
        electionAttempts.inc();
    }

    @Override
    public void onLeaderExpired() {
        if (auditor != null) {
            auditor.shutdown();
            auditor = null;
        }
    }
}
