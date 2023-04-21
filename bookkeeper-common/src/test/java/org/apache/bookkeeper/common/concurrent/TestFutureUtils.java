/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.LongStream;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.junit.Test;

/**
 * Unit Test for {@link FutureUtils}.
 */
public class TestFutureUtils {

    /**
     * Test Exception.
     */
    static class TestException extends IOException {
        private static final long serialVersionUID = -6256482498453846308L;

        public TestException() {
            super("test-exception");
        }
    }

    @Test
    public void testComplete() throws Exception {
        CompletableFuture<Long> future = FutureUtils.createFuture();
        FutureUtils.complete(future, 1024L);
        assertEquals(1024L, FutureUtils.result(future).longValue());
    }

    @Test(expected = TestException.class)
    public void testCompleteExceptionally() throws Exception {
        CompletableFuture<Long> future = FutureUtils.createFuture();
        FutureUtils.completeExceptionally(future, new TestException());
        FutureUtils.result(future);
    }

    @Test
    public void testWhenCompleteAsync() throws Exception {
        OrderedScheduler scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("test-when-complete-async")
            .numThreads(1)
            .build();
        AtomicLong resultHolder = new AtomicLong(0L);
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Long> future = FutureUtils.createFuture();
        FutureUtils.whenCompleteAsync(
            future,
            (result, cause) -> {
                resultHolder.set(result);
                latch.countDown();
            },
            scheduler,
            new Object());
        FutureUtils.complete(future, 1234L);
        latch.await();
        assertEquals(1234L, resultHolder.get());
    }

    @Test
    public void testProxyToSuccess() throws Exception {
        CompletableFuture<Long> src = FutureUtils.createFuture();
        CompletableFuture<Long> target = FutureUtils.createFuture();
        FutureUtils.proxyTo(src, target);
        FutureUtils.complete(src, 10L);
        assertEquals(10L, FutureUtils.result(target).longValue());
    }

    @Test(expected = TestException.class)
    public void testProxyToFailure() throws Exception {
        CompletableFuture<Long> src = FutureUtils.createFuture();
        CompletableFuture<Long> target = FutureUtils.createFuture();
        FutureUtils.proxyTo(src, target);
        FutureUtils.completeExceptionally(src, new TestException());
        FutureUtils.result(target);
    }

    @Test
    public void testVoid() throws Exception {
        CompletableFuture<Void> voidFuture = FutureUtils.Void();
        assertTrue(voidFuture.isDone());
        assertFalse(voidFuture.isCompletedExceptionally());
        assertFalse(voidFuture.isCancelled());
    }

    @Test
    public void testCollectEmptyList() throws Exception {
        List<CompletableFuture<Integer>> futures = Lists.newArrayList();
        List<Integer> result = FutureUtils.result(FutureUtils.collect(futures));
        assertTrue(result.isEmpty());
    }

    @Test
    public void testCollectTenItems() throws Exception {
        List<CompletableFuture<Integer>> futures = Lists.newArrayList();
        List<Integer> expectedResults = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            futures.add(FutureUtils.value(i));
            expectedResults.add(i);
        }
        List<Integer> results = FutureUtils.result(FutureUtils.collect(futures));
        assertEquals(expectedResults, results);
    }

    @Test(expected = TestException.class)
    public void testCollectFailures() throws Exception {
        List<CompletableFuture<Integer>> futures = Lists.newArrayList();
        List<Integer> expectedResults = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            if (i == 9) {
                futures.add(FutureUtils.value(i));
            } else {
                futures.add(FutureUtils.exception(new TestException()));
            }
            expectedResults.add(i);
        }
        FutureUtils.result(FutureUtils.collect(futures));
    }

    @Test
    public void testWithinAlreadyDone() throws Exception {
        OrderedScheduler scheduler = mock(OrderedScheduler.class);
        CompletableFuture<Long> doneFuture = FutureUtils.value(1234L);
        CompletableFuture<Long> withinFuture = FutureUtils.within(
            doneFuture,
            10,
            TimeUnit.MILLISECONDS,
            new TestException(),
            scheduler,
            1234L);
        TimeUnit.MILLISECONDS.sleep(20);
        assertTrue(withinFuture.isDone());
        assertFalse(withinFuture.isCancelled());
        assertFalse(withinFuture.isCompletedExceptionally());
        verify(scheduler, times(0))
            .scheduleOrdered(eq(1234L), isA(Runnable.class), eq(10), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testWithinZeroTimeout() throws Exception {
        OrderedScheduler scheduler = mock(OrderedScheduler.class);
        CompletableFuture<Long> newFuture = FutureUtils.createFuture();
        CompletableFuture<Long> withinFuture = FutureUtils.within(
            newFuture,
            0,
            TimeUnit.MILLISECONDS,
            new TestException(),
            scheduler,
            1234L);
        TimeUnit.MILLISECONDS.sleep(20);
        assertFalse(withinFuture.isDone());
        assertFalse(withinFuture.isCancelled());
        assertFalse(withinFuture.isCompletedExceptionally());
        verify(scheduler, times(0))
            .scheduleOrdered(eq(1234L), isA(Runnable.class), eq(10), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testWithinCompleteBeforeTimeout() throws Exception {
        OrderedScheduler scheduler = mock(OrderedScheduler.class);
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        when(scheduler.scheduleOrdered(any(Object.class), any(Runnable.class), anyLong(), any(TimeUnit.class)))
            .thenAnswer(invocationOnMock -> scheduledFuture);
        CompletableFuture<Long> newFuture = FutureUtils.createFuture();
        CompletableFuture<Long> withinFuture = FutureUtils.within(
            newFuture,
            Long.MAX_VALUE,
            TimeUnit.MILLISECONDS,
            new TestException(),
            scheduler,
            1234L);
        assertFalse(withinFuture.isDone());
        assertFalse(withinFuture.isCancelled());
        assertFalse(withinFuture.isCompletedExceptionally());

        newFuture.complete(5678L);

        assertTrue(withinFuture.isDone());
        assertFalse(withinFuture.isCancelled());
        assertFalse(withinFuture.isCompletedExceptionally());
        assertEquals((Long) 5678L, FutureUtils.result(withinFuture));

        verify(scheduledFuture, times(1))
            .cancel(eq(true));
    }

    @Test
    public void testIgnoreSuccess() {
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        CompletableFuture<Void> ignoredFuture = FutureUtils.ignore(underlyFuture);
        underlyFuture.complete(1234L);
        assertTrue(ignoredFuture.isDone());
        assertFalse(ignoredFuture.isCompletedExceptionally());
        assertFalse(ignoredFuture.isCancelled());
    }

    @Test
    public void testIgnoreFailure() {
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        CompletableFuture<Void> ignoredFuture = FutureUtils.ignore(underlyFuture);
        underlyFuture.completeExceptionally(new TestException());
        assertTrue(ignoredFuture.isDone());
        assertFalse(ignoredFuture.isCompletedExceptionally());
        assertFalse(ignoredFuture.isCancelled());
    }

    @Test
    public void testEnsureSuccess() throws Exception {
        CountDownLatch ensureLatch = new CountDownLatch(1);
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        CompletableFuture<Long> ensuredFuture = FutureUtils.ensure(underlyFuture, () -> {
            ensureLatch.countDown();
        });
        underlyFuture.complete(1234L);
        FutureUtils.result(ensuredFuture);
        assertTrue(ensuredFuture.isDone());
        assertFalse(ensuredFuture.isCompletedExceptionally());
        assertFalse(ensuredFuture.isCancelled());
        ensureLatch.await();
    }

    @Test
    public void testEnsureFailure() throws Exception {
        CountDownLatch ensureLatch = new CountDownLatch(1);
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        CompletableFuture<Long> ensuredFuture = FutureUtils.ensure(underlyFuture, () -> {
            ensureLatch.countDown();
        });
        underlyFuture.completeExceptionally(new TestException());
        FutureUtils.result(FutureUtils.ignore(ensuredFuture));
        assertTrue(ensuredFuture.isDone());
        assertTrue(ensuredFuture.isCompletedExceptionally());
        assertFalse(ensuredFuture.isCancelled());
        ensureLatch.await();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRescueSuccess() throws Exception {
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        Function<Throwable, CompletableFuture<Long>> rescueFuc = mock(Function.class);
        CompletableFuture<Long> rescuedFuture = FutureUtils.rescue(underlyFuture, rescueFuc);
        underlyFuture.complete(1234L);
        FutureUtils.result(rescuedFuture);
        assertTrue(rescuedFuture.isDone());
        assertFalse(rescuedFuture.isCompletedExceptionally());
        assertFalse(rescuedFuture.isCancelled());
        verify(rescueFuc, times(0)).apply(any(Throwable.class));
    }

    @Test
    public void testRescueFailure() throws Exception {
        CompletableFuture<Long> futureCompletedAtRescue = FutureUtils.value(3456L);
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        CompletableFuture<Long> rescuedFuture = FutureUtils.rescue(underlyFuture, (cause) -> futureCompletedAtRescue);
        underlyFuture.completeExceptionally(new TestException());
        FutureUtils.result(rescuedFuture);
        assertTrue(rescuedFuture.isDone());
        assertFalse(rescuedFuture.isCompletedExceptionally());
        assertFalse(rescuedFuture.isCancelled());
        assertEquals((Long) 3456L, FutureUtils.result(rescuedFuture));
    }

    @Test
    public void testStatsSuccess() throws Exception {
        OpStatsLogger statsLogger = mock(OpStatsLogger.class);
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        CompletableFuture<Long> statsFuture = FutureUtils.stats(
            underlyFuture,
            statsLogger,
            Stopwatch.createStarted());
        underlyFuture.complete(1234L);
        FutureUtils.result(statsFuture);
        verify(statsLogger, times(1))
            .registerSuccessfulEvent(anyLong(), eq(TimeUnit.MICROSECONDS));
    }

    @Test
    public void testStatsFailure() throws Exception {
        OpStatsLogger statsLogger = mock(OpStatsLogger.class);
        CompletableFuture<Long> underlyFuture = FutureUtils.createFuture();
        CompletableFuture<Long> statsFuture = FutureUtils.stats(
            underlyFuture,
            statsLogger,
            Stopwatch.createStarted());
        underlyFuture.completeExceptionally(new TestException());
        FutureUtils.result(FutureUtils.ignore(statsFuture));
        verify(statsLogger, times(1))
            .registerFailedEvent(anyLong(), eq(TimeUnit.MICROSECONDS));
    }

    @Test
    public void testProcessListSuccess() throws Exception {
        List<Long> longList = Lists.newArrayList(LongStream.range(0L, 10L).iterator());
        List<Long> expectedList = Lists.transform(
            longList,
            aLong -> 2 * aLong);
        Function<Long, CompletableFuture<Long>> sumFunc = value -> FutureUtils.value(2 * value);
        CompletableFuture<List<Long>> totalFuture = FutureUtils.processList(
            longList,
            sumFunc,
            null);
        assertEquals(expectedList, FutureUtils.result(totalFuture));
    }

    @Test
    public void testProcessEmptyList() throws Exception {
        List<Long> longList = Lists.newArrayList();
        List<Long> expectedList = Lists.transform(
            longList,
            aLong -> 2 * aLong);
        Function<Long, CompletableFuture<Long>> sumFunc = value -> FutureUtils.value(2 * value);
        CompletableFuture<List<Long>> totalFuture = FutureUtils.processList(
            longList,
            sumFunc,
            null);
        assertEquals(expectedList, FutureUtils.result(totalFuture));
    }

    @Test
    public void testProcessListFailures() throws Exception {
        List<Long> longList = Lists.newArrayList(LongStream.range(0L, 10L).iterator());
        AtomicLong total = new AtomicLong(0L);
        Function<Long, CompletableFuture<Long>> sumFunc = value -> {
            if (value < 5) {
                total.addAndGet(value);
                return FutureUtils.value(2 * value);
            } else {
                return FutureUtils.exception(new TestException());
            }
        };
        CompletableFuture<List<Long>> totalFuture = FutureUtils.processList(
            longList,
            sumFunc,
            null);
        try {
            FutureUtils.result(totalFuture);
            fail("Should fail with TestException");
        } catch (TestException te) {
            // as expected
        }
        assertEquals(10L, total.get());
    }

}
