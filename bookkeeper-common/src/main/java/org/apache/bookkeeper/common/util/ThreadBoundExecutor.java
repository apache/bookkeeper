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
package org.apache.bookkeeper.common.util;

import java.util.concurrent.ExecutorService;

/**
 * An {@link ExecutorService} backed by a single thread, which can tell whether the caller is already on that
 * thread and then run a task inline instead of queueing it.
 *
 * <p>Implemented by {@link SingleThreadExecutor} and by the threads an {@link OrderedExecutor} hands out from
 * {@link OrderedExecutor#chooseThread(long)}, which are decorated when task tracing or MDC preservation is enabled.
 */
public interface ThreadBoundExecutor extends ExecutorService {

    /**
     * Whether the calling thread is the thread of this executor.
     */
    boolean isCurrentThread();

    /**
     * Runs the task inline when called from this executor's own thread, otherwise submits it like
     * {@link #execute(Runnable)}. Like {@code execute}, it rejects the task once the executor is shut down.
     *
     * <p>The inline run bypasses the queue: a task submitted this way from the executor thread runs before the
     * tasks already queued, nested inside the task that submitted it. Use it only where that reordering is
     * acceptable.
     */
    void executeOrRun(Runnable r);
}
