/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.util;

import static org.apache.bookkeeper.common.util.ExceptionUtils.toIOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.exceptions.ObjectClosedException;
import org.junit.Test;

/**
 * Test Case for {@link ExceptionUtils}.
 */
public class TestExceptionUtils {

    @Test
    public void testCallAndHandleClosedAsync() throws Exception {
        String componentName = "test-component";
        CompletableFuture<Long> closedFuture = ExceptionUtils.callAndHandleClosedAsync(
            componentName,
            true,
            (future) -> {
                future.complete(10L);
            });
        try {
            FutureUtils.result(closedFuture);
            fail("Should fail with object closed exception");
        } catch (ObjectClosedException oce) {
            assertEquals(componentName + " is already closed.", oce.getMessage());
        }

        CompletableFuture<Long> noneClosedFuture = ExceptionUtils.callAndHandleClosedAsync(
            componentName,
            false,
            (future) -> {
                future.complete(10L);
            });
        assertEquals(10L, FutureUtils.result(noneClosedFuture).longValue());
    }

    @Test
    public void testToIOException() {
        IOException ioe = new IOException("Test-IOE");
        assertTrue(ioe == toIOException(ioe));
        Exception se = new Exception("Test-DLSE");
        IOException ioe2 = toIOException(se);
        assertEquals("java.lang.Exception: Test-DLSE", ioe2.getMessage());
        assertTrue(se == ioe2.getCause());
        ExecutionException ee = new ExecutionException(ioe);
        assertTrue(ioe == toIOException(ee));
        CompletionException ce = new CompletionException(ioe);
        assertTrue(ioe == toIOException(ce));
    }

}
