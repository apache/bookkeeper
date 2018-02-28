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
package org.apache.bookkeeper.clients.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;

/**
 * Define a set of resources used for storage.
 */
public class ClientResources {

    private static final ClientResources SHARED = create();

    public static ClientResources shared() {
        return SHARED;
    }

    public static ClientResources create() {
        return new ClientResources();
    }

    private final Resource<OrderedScheduler> scheduler;
    private final Resource<HashedWheelTimer> timer;
    private final Resource<ExecutorService> executor;

    private ClientResources() {
        this.scheduler =
            new Resource<OrderedScheduler>() {

                private static final String name = "client-scheduler";

                @Override
                public OrderedScheduler create() {
                    return OrderedScheduler.newSchedulerBuilder()
                        .numThreads(Runtime.getRuntime().availableProcessors() * 2)
                        .name(name)
                        .build();
                }

                @Override
                public void close(OrderedScheduler instance) {
                    instance.shutdown();
                }

                @Override
                public String toString() {
                    return name;
                }
            };

        this.timer =
            new Resource<HashedWheelTimer>() {

                private static final String name = "client-timer";

                @Override
                public HashedWheelTimer create() {
                    HashedWheelTimer timer = new HashedWheelTimer(
                        new ThreadFactoryBuilder()
                            .setNameFormat(name + "-%d")
                            .build(),
                        200,
                        TimeUnit.MILLISECONDS,
                        512,
                        true);
                    timer.start();
                    return timer;
                }

                @Override
                public void close(HashedWheelTimer instance) {
                    instance.stop();
                }

                @Override
                public String toString() {
                    return name;
                }
            };

        this.executor = new Resource<ExecutorService>() {

            private static final String name = "stream-client-executor";

            @Override
            public ExecutorService create() {
                return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
            }

            @Override
            public void close(ExecutorService instance) {
                instance.shutdown();
            }

            @Override
            public String toString() {
                return name;
            }
        };
    }

    public Resource<OrderedScheduler> scheduler() {
        return scheduler;
    }

    public Resource<HashedWheelTimer> timer() {
        return timer;
    }

    public Resource<ExecutorService> executor() {
        return executor;
    }

}
