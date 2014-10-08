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
package org.apache.bookkeeper.stats.twitter.ostrich;

import com.twitter.ostrich.admin.CustomHttpHandler;
import com.twitter.ostrich.admin.RuntimeEnvironment;
import com.twitter.util.Duration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import scala.Some;
import scala.collection.immutable.$colon$colon;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Map;
import scala.collection.immutable.Map$;

import java.util.concurrent.TimeUnit;

public class OstrichProvider implements StatsProvider {

    protected final static String STATS_EXPORT = "statsExport";
    protected final static String STATS_HTTP_PORT = "statsHttpPort";

    private com.twitter.ostrich.admin.AdminHttpService statsExporter = null;

    private static <T> List<T> list(T ... ts) {
        List<T> result = List$.MODULE$.empty();
        for (int i = ts.length; i > 0; i--) {
            result = new $colon$colon<T>(ts[i-1], result);
        }
        return result;
    }

    private static <K, V> Map<K, V> emptyMap() {
        Map<K, V> result = Map$.MODULE$.empty();
        return result;
    }

    @Override
    public void start(Configuration conf) {
        if (conf.getBoolean(STATS_EXPORT, false)) {
            statsExporter = new com.twitter.ostrich.admin.AdminServiceFactory(
                    conf.getInt(STATS_HTTP_PORT, 9002), 20, null, Some.apply(""), null,
                    OstrichProvider.<String, CustomHttpHandler>emptyMap(), list(Duration.apply(1, TimeUnit.MINUTES))
            ).apply(RuntimeEnvironment.apply(this, new String[0]));
        }
        if (null != statsExporter) {
            statsExporter.start();
        }
    }

    @Override
    public void stop() {
        if (null != statsExporter) {
            statsExporter.shutdown();
        }
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        return new OstrichStatsLoggerImpl(scope, com.twitter.ostrich.stats.Stats.get(""));
    }
}
