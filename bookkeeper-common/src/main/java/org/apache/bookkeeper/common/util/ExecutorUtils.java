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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ThreadFactory;

/**
 * Executor/Thread related utils.
 */
public class ExecutorUtils {

    /**
     * Get a {@link ThreadFactory} suitable for use in the current environment.
     *
     * @param nameFormat to apply to threads created by the factory.
     * @param daemon     {@code true} if the threads the factory creates are daemon threads,
     *                   {@code false} otherwise.
     * @return a {@link ThreadFactory}.
     */
    public static ThreadFactory getThreadFactory(String nameFormat, boolean daemon) {
        ThreadFactory threadFactory = MoreExecutors.platformThreadFactory();
        return new ThreadFactoryBuilder()
            .setThreadFactory(threadFactory)
            .setDaemon(daemon)
            .setNameFormat(nameFormat)
            .build();
    }

}
