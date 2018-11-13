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
package org.apache.bookkeeper.stats.twitter.science;

import org.apache.bookkeeper.stats.CachingStatsProvider;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A stats provider implemented based on <i>Twitter Stats</i> library.
 */
public class TwitterStatsProvider implements StatsProvider {

    static final Logger LOG = LoggerFactory.getLogger(TwitterStatsProvider.class);

    protected static final String STATS_EXPORT = "statsExport";
    protected static final String STATS_HTTP_PORT = "statsHttpPort";

    private HTTPStatsExporter statsExporter = null;
    private final CachingStatsProvider cachingStatsProvider;

    public TwitterStatsProvider() {
        this.cachingStatsProvider = new CachingStatsProvider(new StatsProvider() {

            @Override
            public void start(Configuration conf) {
                // nop
            }

            @Override
            public void stop() {
                // nop
            }

            @Override
            public StatsLogger getStatsLogger(String scope) {
                return new TwitterStatsLoggerImpl(scope);
            }

            @Override
            public String getStatsName(String... statsComponents) {
                return StringUtils.join(statsComponents, '_').toLowerCase();
            }
        });
    }

    @Override
    public void start(Configuration conf) {
        if (conf.getBoolean(STATS_EXPORT, false)) {
            statsExporter = new HTTPStatsExporter(conf.getInt(STATS_HTTP_PORT, 9002));
        }
        if (null != statsExporter) {
            try {
                statsExporter.start();
            } catch (Exception e) {
                LOG.error("Fail to start stats exporter : ", e);
            }
        }
    }

    @Override
    public void stop() {
        if (null != statsExporter) {
            try {
                statsExporter.stop();
            } catch (Exception e) {
                LOG.error("Fail to stop stats exporter : ", e);
            }
        }
    }

    @Override
    public StatsLogger getStatsLogger(String name) {
        return this.cachingStatsProvider.getStatsLogger(name);
    }

    @Override
    public String getStatsName(String... statsComponents) {
        return this.cachingStatsProvider.getStatsName(statsComponents);
    }
}
