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

import static org.apache.bookkeeper.common.util.SafeRunnable.safeRun;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.AdditionalAnswers.answerVoid;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.NullAppender;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that decorators applied by OrderedExecutor/Scheduler are correctly applied.
 */
public class TestOrderedExecutorDecorators {
    private static final Logger log = LoggerFactory.getLogger(TestOrderedExecutorDecorators.class);
    private static final String MDC_KEY = "mdc-key";

    private NullAppender mockAppender;
    private final Queue<String> capturedEvents = new ConcurrentLinkedQueue<>();

    public static String mdcFormat(Object mdc, String message) {
        return String.format("[%s:%s] %s", MDC_KEY, mdc, message);
    }

    @Before
    public void setUp() throws Exception {
        ThreadContext.clearMap();
        LoggerContext lc = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        mockAppender = spy(NullAppender.createAppender(UUID.randomUUID().toString()));
        mockAppender.start();
        lc.getConfiguration().addAppender(mockAppender);
        lc.getRootLogger().addAppender(lc.getConfiguration().getAppender(mockAppender.getName()));
        lc.getConfiguration().getRootLogger().setLevel(Level.INFO);
        lc.updateLoggers();

        doAnswer(answerVoid((LogEvent event) -> {
                    capturedEvents.add(mdcFormat(event.getContextData().getValue(MDC_KEY),
                                                 event.getMessage().getFormattedMessage()));
                })).when(mockAppender).append(any());
    }

    @After
    public void tearDown() throws Exception {
        LoggerContext lc = (LoggerContext) org.apache.logging.log4j.LogManager.getContext(false);
        lc.getRootLogger().removeAppender(lc.getConfiguration().getAppender(mockAppender.getName()));
        lc.updateLoggers();
        capturedEvents.clear();
        ThreadContext.clearMap();
    }

    @Test
    public void testMDCInvokeOrdered() throws Exception {
        OrderedExecutor executor = OrderedExecutor.newBuilder()
            .name("test").numThreads(20).preserveMdcForTaskExecution(true).build();

        try {
            ThreadContext.put(MDC_KEY, "testMDCInvokeOrdered");
            executor.submitOrdered(10, () -> {
                    log.info("foobar");
                    return 10;
                }).get();
            assertThat(capturedEvents,
                       hasItem(mdcFormat("testMDCInvokeOrdered", "foobar")));
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void testMDCInvokeDirectOnChosen() throws Exception {
        OrderedExecutor executor = OrderedExecutor.newBuilder()
            .name("test").numThreads(20).preserveMdcForTaskExecution(true).build();

        try {
            ThreadContext.put(MDC_KEY, "testMDCInvokeOrdered");
            executor.chooseThread(10).submit(() -> {
                    log.info("foobar");
                    return 10;
                }).get();
            assertThat(capturedEvents,
                       hasItem(mdcFormat("testMDCInvokeOrdered", "foobar")));
        } finally {
            executor.shutdown();
        }

    }


    @Test
    public void testMDCScheduleOrdered() throws Exception {
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test").numThreads(20).preserveMdcForTaskExecution(true).build();

        try {
            ThreadContext.put(MDC_KEY, "testMDCInvokeOrdered");
            scheduler.scheduleOrdered(10, safeRun(() -> {
                        log.info("foobar");
                    }), 0, TimeUnit.DAYS).get();
            assertThat(capturedEvents,
                       hasItem(mdcFormat("testMDCInvokeOrdered", "foobar")));
        } finally {
            scheduler.shutdown();
        }
    }

    @Test
    public void testMDCScheduleDirectOnChosen() throws Exception {
                OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test").numThreads(20).preserveMdcForTaskExecution(true).build();

        try {
            ThreadContext.put(MDC_KEY, "testMDCInvokeOrdered");
            scheduler.chooseThread(10).schedule(safeRun(() -> {
                        log.info("foobar");
                    }), 0, TimeUnit.DAYS).get();
            assertThat(capturedEvents,
                       hasItem(mdcFormat("testMDCInvokeOrdered", "foobar")));
        } finally {
            scheduler.shutdown();
        }
    }

}
