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
package org.apache.bookkeeper.server.service;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.bookie.ScrubberStats.DETECTED_FATAL_SCRUB_ERRORS;
import static org.apache.bookkeeper.bookie.ScrubberStats.DETECTED_SCRUB_ERRORS;
import static org.apache.bookkeeper.bookie.ScrubberStats.RUN_DURATION;

import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.server.component.ServerLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.bookkeeper.common.component.LifecycleComponent} that runs the scrubber background service.
 */
public class ScrubberService extends ServerLifecycleComponent {
    private static final Logger LOG = LoggerFactory.getLogger(ScrubberService.class);

    private static final String NAME = "scrubber";
    private final ScheduledExecutorService executor;
    private final Random rng = new Random();
    private final long scrubPeriod;
    private final Optional<RateLimiter> scrubRateLimiter;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final LedgerStorage ledgerStorage;

    private final OpStatsLogger scrubCounter;
    private final Counter errorCounter;
    private final Counter fatalErrorCounter;

    public ScrubberService(
            StatsLogger logger,
            BookieConfiguration conf,
            LedgerStorage ledgerStorage) {
        super(NAME, conf, logger);
        this.executor = Executors.newSingleThreadScheduledExecutor(
                new DefaultThreadFactory("ScrubThread"));

        this.scrubPeriod = conf.getServerConf().getLocalScrubPeriod();
        checkArgument(
                scrubPeriod > 0,
                "localScrubInterval must be > 0 for ScrubberService to be used");

        double rateLimit = conf.getServerConf().getLocalScrubRateLimit();
        this.scrubRateLimiter = rateLimit == 0 ? Optional.empty() : Optional.of(RateLimiter.create(rateLimit));

        this.ledgerStorage = ledgerStorage;

        this.scrubCounter = logger.getOpStatsLogger(RUN_DURATION);
        this.errorCounter = logger.getCounter(DETECTED_SCRUB_ERRORS);
        this.fatalErrorCounter = logger.getCounter(DETECTED_FATAL_SCRUB_ERRORS);
    }

    private long getNextPeriodMS() {
        return (long) (((double) scrubPeriod) * (1.5 - rng.nextDouble()) * 1000);
    }

    private void doSchedule() {
        executor.schedule(
                this::run,
                getNextPeriodMS(),
                TimeUnit.MILLISECONDS);

    }

    private void run() {
        boolean success = false;
        long start = MathUtils.nowInNano();
        try {
            List<LedgerStorage.DetectedInconsistency> errors = ledgerStorage.localConsistencyCheck(scrubRateLimiter);
            if (errors.size() > 0) {
                errorCounter.add(errors.size());
                LOG.error("Found inconsistency during localConsistencyCheck:");
                for (LedgerStorage.DetectedInconsistency error : errors) {
                    LOG.error("Ledger {}, entry {}: ", error.getLedgerId(), error.getEntryId(), error.getException());
                }
            }
            success = true;
        } catch (IOException e) {
            fatalErrorCounter.inc();
            LOG.error("Got fatal exception {} running localConsistencyCheck", e.toString());
        }
        if (success) {
            scrubCounter.registerSuccessfulEvent(MathUtils.elapsedNanos(start), TimeUnit.NANOSECONDS);
        } else {
            scrubCounter.registerFailedEvent(MathUtils.elapsedNanos(start), TimeUnit.NANOSECONDS);
            Runtime.getRuntime().exit(ExitCode.BOOKIE_EXCEPTION);
        }
        if (!stop.get()) {
            doSchedule();
        }
    }

    @Override
    protected void doStart() {
        doSchedule();
    }

    @Override
    protected void doStop() {
        stop.set(true);
        executor.shutdown();
    }

    @Override
    protected void doClose() throws IOException {
        // no-op
    }
}
