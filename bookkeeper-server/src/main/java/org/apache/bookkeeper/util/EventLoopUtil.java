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
package org.apache.bookkeeper.util;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.SelectStrategy;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import java.util.concurrent.ThreadFactory;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.affinity.CpuAffinity;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;


/**
 * Utility class to initialize Netty event loops.
 */
@Slf4j
@UtilityClass
public class EventLoopUtil {

    private static final String ENABLE_IO_URING = "enable.io_uring";

    public static EventLoopGroup getClientEventLoopGroup(ClientConfiguration conf, ThreadFactory threadFactory) {
        return getEventLoopGroup(threadFactory, conf.getNumIOThreads(), conf.isBusyWaitEnabled());
    }

    public static EventLoopGroup getServerEventLoopGroup(ServerConfiguration conf, ThreadFactory threadFactory) {
        return getEventLoopGroup(threadFactory, conf.getServerNumIOThreads(), conf.isBusyWaitEnabled());
    }

    public static EventLoopGroup getServerAcceptorGroup(ServerConfiguration conf, ThreadFactory threadFactory) {
        return getEventLoopGroup(threadFactory, conf.getServerNumAcceptorThreads(), false);
    }

    private static EventLoopGroup getEventLoopGroup(ThreadFactory threadFactory,
            int numThreads, boolean enableBusyWait) {
        if (!SystemUtils.IS_OS_LINUX) {
            return new NioEventLoopGroup(numThreads, threadFactory);
        }

        String enableIoUring = System.getProperty(ENABLE_IO_URING);

        // By default, io_uring will not be enabled, even if available. The environment variable will be used:
        // enable.io_uring=1
        if (StringUtils.equalsAnyIgnoreCase(enableIoUring, "1", "true")) {
            // Throw exception if IOUring cannot be used
            IOUring.ensureAvailability();
            return new IOUringEventLoopGroup(numThreads, threadFactory);
        } else {
            try {
                if (!enableBusyWait) {
                    // Regular Epoll based event loop
                    return new EpollEventLoopGroup(numThreads, threadFactory);
                }

                // With low latency setting, put the Netty event loop on busy-wait loop to reduce cost of
                // context switches
                EpollEventLoopGroup eventLoopGroup = new EpollEventLoopGroup(numThreads, threadFactory,
                        () -> (selectSupplier, hasTasks) -> SelectStrategy.BUSY_WAIT);

                // Enable CPU affinity on IO threads
                for (int i = 0; i < numThreads; i++) {
                    eventLoopGroup.next().submit(() -> {
                        try {
                            CpuAffinity.acquireCore();
                        } catch (Throwable t) {
                            log.warn("Failed to acquire CPU core for thread {} err {} {}",
                                    Thread.currentThread().getName(), t.getMessage(), t);
                        }
                    });
                }

                return eventLoopGroup;
            } catch (ExceptionInInitializerError | NoClassDefFoundError | UnsatisfiedLinkError e) {
                log.warn("Could not use Netty Epoll event loop: {}", e.getMessage());
                return new NioEventLoopGroup(numThreads, threadFactory);
            }
        }
    }
}
