/*
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
 */
package org.apache.bookkeeper.statelib.testing.executors;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test {@link MockExecutorController}.
 */
public class MockExecutorControllerWithSchedulerTest {

    private static final int MAX_SCHEDULES = 5;

    private ScheduledExecutorService mockExecutor;
    private ScheduledExecutorService executor;
    private MockExecutorController mockExecutorControl;

    @Before
    public void setup() {
        this.executor = Executors.newSingleThreadScheduledExecutor();
        this.mockExecutor = mock(ScheduledExecutorService.class);
        this.mockExecutorControl = new MockExecutorController(executor)
            .controlExecute(mockExecutor)
            .controlSubmit(mockExecutor)
            .controlSchedule(mockExecutor)
            .controlScheduleAtFixedRate(mockExecutor, MAX_SCHEDULES);
    }

    @After
    public void teardown() {
        if (null != executor) {
            executor.shutdown();
        }
    }

    @Test
    public void testSubmit() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        mockExecutor.submit(task);
        verify(task, times(1)).run();
    }

    @Test
    public void testExecute() throws ExecutionException, InterruptedException, TimeoutException {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        Runnable task = mock(Runnable.class);
        doAnswer(x -> future.complete(null)).when(task).run();
        mockExecutor.execute(task);
        future.get(5000, TimeUnit.MILLISECONDS);
        verify(task, times(1)).run();
    }

    @Test
    public void testDelay() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        mockExecutor.schedule(task, 10, TimeUnit.MILLISECONDS);
        mockExecutorControl.advance(Duration.ofMillis(5));
        verify(task, times(0)).run();
        mockExecutorControl.advance(Duration.ofMillis(10));
        verify(task, times(1)).run();
    }

    @Test
    public void testScheduleAtFixedRate() {
        Runnable task = mock(Runnable.class);
        doNothing().when(task).run();
        mockExecutor.scheduleAtFixedRate(task, 5, 10, TimeUnit.MILLISECONDS);

        // first delay
        mockExecutorControl.advance(Duration.ofMillis(2));
        verify(task, times(0)).run();
        mockExecutorControl.advance(Duration.ofMillis(3));
        verify(task, times(1)).run();

        // subsequent delays
        for (int i = 1; i < MAX_SCHEDULES; i++) {
            mockExecutorControl.advance(Duration.ofMillis(2));
            verify(task, times(i)).run();
            mockExecutorControl.advance(Duration.ofMillis(8));
            verify(task, times(i + 1)).run();
        }

        // no more invocations
        mockExecutorControl.advance(Duration.ofMillis(500));
        verify(task, times(MAX_SCHEDULES)).run();
    }

}
