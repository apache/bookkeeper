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

import java.io.IOException;
import java.io.Writer;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * Provider to provide stats logger for different scopes.
 */
public interface StatsProvider {
    /**
     * Intialize the stats provider by loading the given configuration <i>conf</i>.
     *
     * @param conf
     *          Configuration to configure the stats provider.
     */
    void start(Configuration conf);

    /**
     * Close the stats provider.
     */
    void stop();

    /**
     *
     * @param writer
     * @throws IOException
     */
    default void writeAllMetrics(Writer writer) throws IOException {
        throw new UnsupportedOperationException("writeAllMetrics is not implemented yet");
    }

    /**
     * Return the stats logger to a given <i>scope</i>.
     * @param scope
     *          Scope for the given stats
     * @return stats logger for the given <i>scope</i>
     */
    StatsLogger getStatsLogger(String scope);

    /**
     * Return the fully qualified stats name comprised of given <tt>statsComponents</tt>.
     *
     * @param statsComponents stats components to comprise the fully qualified stats name
     * @return the fully qualified stats name
     */
    default String getStatsName(String...statsComponents) {
        return StringUtils.join(statsComponents, '/');
    }
}
