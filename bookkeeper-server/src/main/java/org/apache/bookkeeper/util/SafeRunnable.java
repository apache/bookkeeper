/**
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
package org.apache.bookkeeper.util;

import java.util.function.Consumer;

/**
 * A SafeRunnable implementation.
 */
public abstract class SafeRunnable implements org.apache.bookkeeper.common.util.SafeRunnable {

    /**
     * Utility method to use SafeRunnable from lambdas.
     *
     * <p>Eg:
     * <pre>
     * <code>
     * executor.submit(SafeRunnable.safeRun(() -> {
     *    // My not-safe code
     * });
     * </code>
     * </pre>
     */
    public static SafeRunnable safeRun(Runnable runnable) {
        return new SafeRunnable() {
            @Override
            public void safeRun() {
                runnable.run();
            }
        };
    }

    /**
     * Utility method to use SafeRunnable from lambdas with
     * a custom exception handler.
     *
     * <p>Eg:
     * <pre>
     * <code>
     * executor.submit(SafeRunnable.safeRun(() -> {
     *    // My not-safe code
     * }, exception -> {
     *    // Handle exception
     * );
     * </code>
     * </pre>
     *
     * @param runnable
     * @param exceptionHandler
     *            handler that will be called when there are any exception
     * @return
     */
    public static SafeRunnable safeRun(Runnable runnable, Consumer<Throwable> exceptionHandler) {
        return new SafeRunnable() {
            @Override
            public void safeRun() {
                try {
                    runnable.run();
                } catch (Throwable t) {
                    exceptionHandler.accept(t);
                    throw t;
                }
            }
        };
    }
}
