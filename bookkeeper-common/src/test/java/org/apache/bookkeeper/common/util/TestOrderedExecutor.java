/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.common.util;

import org.apache.bookkeeper.test.TestStatsProvider;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test OrderedExecutor/Scheduler .
 */
public class TestOrderedExecutor {

    @Test
    public void testOrderExecutorPrometheusMetric() {
        testGenerateMetric(false);
        testGenerateMetric(true);
    }

    private void testGenerateMetric(boolean isTraceTaskExecution) {
        TestStatsProvider provider = new TestStatsProvider();

        TestStatsProvider.TestStatsLogger rootStatsLogger = provider.getStatsLogger("");
        TestStatsProvider.TestStatsLogger bookieStats =
                (TestStatsProvider.TestStatsLogger) rootStatsLogger.scope("bookkeeper_server");

        OrderedExecutor executor = OrderedExecutor.newBuilder().statsLogger(bookieStats)
                .name("test").numThreads(1).traceTaskExecution(isTraceTaskExecution).build();

        TestStatsProvider.TestStatsLogger testStatsLogger = (TestStatsProvider.TestStatsLogger)
                bookieStats.scope("thread_0");

        Assert.assertNotNull(testStatsLogger.getGauge("test-queue").getSample());
        Assert.assertNotNull(testStatsLogger.getGauge("test-rejected-tasks").getSample());
        Assert.assertNotNull(testStatsLogger.getGauge("test-failed-tasks").getSample());
        Assert.assertNotNull(testStatsLogger.getGauge("test-completed-tasks").getSample());
    }
}
