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

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;

/**
 * A {@link StatsProvider} implemented based on <i>Codahale</i> metrics library.
 */
@SuppressWarnings("deprecation")
public class FastCodahaleMetricsProvider extends CodahaleMetricsProvider {

    @Override
    public StatsLogger getStatsLogger(String name) {
        initIfNecessary();
        return new FastCodahaleStatsLogger(getMetrics(), name);
    }
}
