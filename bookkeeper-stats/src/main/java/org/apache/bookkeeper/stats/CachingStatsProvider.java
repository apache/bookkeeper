package org.apache.bookkeeper.stats;

import org.apache.commons.configuration.Configuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CachingStatsProvider implements StatsProvider {

    protected final StatsProvider underlying;
    protected final ConcurrentMap<String, StatsLogger> statsLoggers;

    public CachingStatsProvider(StatsProvider provider) {
        this.underlying = provider;
        this.statsLoggers = new ConcurrentHashMap<String, StatsLogger>();
    }

    @Override
    public void start(Configuration conf) {
        this.underlying.start(conf);
    }

    @Override
    public void stop() {
        this.underlying.stop();
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        StatsLogger statsLogger = statsLoggers.get(scope);
        if (null == statsLogger) {
            StatsLogger newStatsLogger =
                    new CachingStatsLogger(underlying.getStatsLogger(scope));
            StatsLogger oldStatsLogger = statsLoggers.putIfAbsent(scope, newStatsLogger);
            statsLogger = (null == oldStatsLogger) ? newStatsLogger : oldStatsLogger;
        }
        return statsLogger;
    }
}
