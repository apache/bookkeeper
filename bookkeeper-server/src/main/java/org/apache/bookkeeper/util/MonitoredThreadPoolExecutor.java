package org.apache.bookkeeper.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.bookkeeper.stats.BookkeeperStatsLogger;
import org.apache.bookkeeper.stats.Gauge;

public class MonitoredThreadPoolExecutor extends ThreadPoolExecutor {

    public MonitoredThreadPoolExecutor(int numThreads, String nameFormat, BookkeeperStatsLogger statsLogger, Enum e) {
        super(numThreads,
            numThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder().setNameFormat(nameFormat).build());

        // outstanding requests
        statsLogger.registerGauge(e, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                try {
                    return getTaskCount() - getCompletedTaskCount();
                } catch (RuntimeException exc) {
                    return 0;
                }
            }
        });
    }
}
