/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.bookkeeper.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to raise alert when we detect an event that should never happen in production.
 */
public class AlertStatsLogger {
    private static final Logger logger = LoggerFactory.getLogger(AlertStatsLogger.class);

    public final String alertStatName;

    private final StatsLogger globalStatsLogger;
    private final StatsLogger scopedStatsLogger;
    private final String scope;
    private Counter globalCounter = null;
    private Counter scopedCounter = null;

    public AlertStatsLogger(StatsLogger globalStatsLogger, String scope, String alertStatName) {
        this.globalStatsLogger = globalStatsLogger;
        this.scope = scope;
        this.scopedStatsLogger = globalStatsLogger.scope(scope);
        this.alertStatName = alertStatName;
    }

    public AlertStatsLogger(StatsLogger globalStatsLogger, String alertStatName) {
        this.globalStatsLogger = globalStatsLogger;
        this.scope = null;
        this.scopedStatsLogger = null;
        this.alertStatName = alertStatName;
    }

    private String format(String msg) {
        return msg.startsWith("ALERT!: ") ? msg :
                ("ALERT!: " + (scope != null ? "(" + scope + "):" : "") + msg);
    }

    private void initializeCountersIfNeeded() {
        if (null != globalCounter) {
            return;
        }

        globalCounter = globalStatsLogger.getCounter(alertStatName);

        if (null != scopedStatsLogger) {
            scopedCounter = scopedStatsLogger.getCounter(alertStatName);
        }
    }

    /**
     * Report an alertable condition". Prefixes "ALERT!: " if not already prefixed.
     */
    public void raise(String msg, Object... args) {
        initializeCountersIfNeeded();
        globalCounter.inc();
        if (null != scopedCounter) {
            scopedCounter.inc();
        }
        logger.error(format(msg), args);
        logger.error("fake exception to generate stack trace", new Exception());
    }
}
