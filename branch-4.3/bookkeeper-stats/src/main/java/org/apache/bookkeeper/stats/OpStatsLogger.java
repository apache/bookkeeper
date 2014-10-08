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

/**
 * This interface handles logging of statistics related to each operation (PUBLISH,
 * CONSUME etc.)
 */
public interface OpStatsLogger {

    /**
     * Increment the failed op counter with the given eventLatencyMillis.
     * @param eventLatencyMillis The event latency in milliseconds.
     */
    public void registerFailedEvent(long eventLatencyMillis);

    /**
     * An operation succeeded with the given eventLatencyMillis. Update
     * stats to reflect the same
     * @param eventLatencyMillis The event latency in milliseconds.
     */
    public void registerSuccessfulEvent(long eventLatencyMillis);

    /**
     * @return Returns an OpStatsData object with necessary values. We need this function
     * to support JMX exports. This should be deprecated sometime in the near future.
     * populated.
     */
    public OpStatsData toOpStatsData();

    /**
     * Clear stats for this operation.
     */
    public void clear();
}
