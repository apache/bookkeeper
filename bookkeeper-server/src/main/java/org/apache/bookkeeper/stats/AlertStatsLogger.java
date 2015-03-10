package org.apache.bookkeeper.stats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to raise alert when we detect an event that should never happen in production
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
                ("ALERT!: " + (scope != null ? "(" + scope + "):" : "" ) + msg);
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
