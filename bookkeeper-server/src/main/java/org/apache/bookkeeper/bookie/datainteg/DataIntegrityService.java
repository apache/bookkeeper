/*
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
 */

package org.apache.bookkeeper.bookie.datainteg;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.component.AbstractLifecycleComponent;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * An abstract lifecycle component that can perform data integrity checking.
 */
@Slf4j
public class DataIntegrityService extends AbstractLifecycleComponent<BookieConfiguration> {
    private final DataIntegrityCheck check;
    private final ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;

    public DataIntegrityService(BookieConfiguration conf,
                                StatsLogger statsLogger,
                                DataIntegrityCheck check) {
        super("data-integ", conf, statsLogger);
        this.check = check;
        scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                .setNameFormat("bookie-data-integ-%d")
                .setUncaughtExceptionHandler(
                        (t, ex) -> log.error("Event: {}, thread: {}",
                                Events.DATA_INTEG_SERVICE_UNCAUGHT_ERROR,
                                t, ex))
                .build());
        scheduledFuture = null;
    }

    // allow tests to reduce interval
    protected int interval() {
        return 3;
    }

    protected TimeUnit intervalUnit() {
        return TimeUnit.SECONDS;
    }

    @Override
    protected void doStart() {
        log.info("Event: {}, interval: {}, intervalUnit: {}",
                        Events.DATA_INTEG_SERVICE_START, interval(), intervalUnit());
        synchronized (this) {
            scheduledFuture = scheduler.scheduleAtFixedRate(() -> {
                    try {
                        if (check.needsFullCheck()) {
                            check.runFullCheck().get();
                        }
                    } catch (InterruptedException ie) {
                        log.warn("Event: {}", Events.DATA_INTEG_SERVICE_INTERRUPTED, ie);
                        Thread.currentThread().interrupt();
                    } catch (Throwable t) {
                        log.error("Event: {}", Events.DATA_INTEG_SERVICE_ERROR, t);
                    }
                }, 0, interval(), intervalUnit());
        }
    }

    @Override
    protected void doStop() {
        log.info("Event: {}", Events.DATA_INTEG_SERVICE_STOP);
        synchronized (this) {
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
                scheduledFuture = null;
            }
        }
    }

    @Override
    protected void doClose() throws IOException {
        synchronized (this) {
            // just in case stop didn't get called, the scheduledfuture
            // would stop the scheduler from shutting down
            if (scheduledFuture != null) {
                scheduledFuture.cancel(true);
                scheduledFuture = null;
            }
        }

        scheduler.shutdown();
    }
}
