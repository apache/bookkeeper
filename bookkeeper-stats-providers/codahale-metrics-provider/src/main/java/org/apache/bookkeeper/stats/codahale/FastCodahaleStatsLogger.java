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
package org.apache.bookkeeper.stats.codahale;

import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * A {@link StatsLogger} implemented based on <i>Codahale</i> metrics library.
 */
public class FastCodahaleStatsLogger extends CodahaleStatsLogger {

    private static final ConcurrentHashMap<String, CodahaleOpStatsLogger> statsLoggerCache =
            new ConcurrentHashMap<String, CodahaleOpStatsLogger>();

    FastCodahaleStatsLogger(MetricRegistry metrics, String basename) {
        super(metrics, basename);
    }

    @Override
    @SuppressFBWarnings(
            value = {
                    "JLM_JSR166_UTILCONCURRENT_MONITORENTER",
                    "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION"
            },
            justification = "We use synchronized (statsLoggerCache) to make get/put atomic")
    public OpStatsLogger getOpStatsLogger(String statName) {
        CodahaleOpStatsLogger logger;
        String nameSuccess = name(basename, statName);
        logger = statsLoggerCache.get(nameSuccess);
        if (logger == null) {
            synchronized (statsLoggerCache) {
                // check again now that we have the lock
                logger = statsLoggerCache.get(nameSuccess);
                if (logger == null) {
                    String nameFailure = name(basename, statName + "-fail");
                    FastTimer success;
                    FastTimer failure;
                    Map<String, Timer> timers = metrics.getTimers();
                    success = timers != null ? (FastTimer) timers.get(nameSuccess) : null;
                    if (success == null) {
                        success = new FastTimer(60, FastTimer.Buckets.fine);
                        metrics.register(nameSuccess, success);
                    }
                    failure = timers != null ? (FastTimer) timers.get(nameFailure) : null;
                    if (failure == null) {
                        failure = new FastTimer(60, FastTimer.Buckets.coarse);
                        metrics.register(nameFailure, failure);
                    }
                    logger = new CodahaleOpStatsLogger(success, failure);
                    statsLoggerCache.put(nameSuccess, logger);
                }
            }
        }
        return logger;
    }

    @Override
    public StatsLogger scope(String scope) {
        String scopeName;
        if (basename == null || 0 == basename.length()) {
            scopeName = scope;
        } else {
            scopeName = name(basename, scope);
        }
        return new FastCodahaleStatsLogger(metrics, scopeName);
    }

}
