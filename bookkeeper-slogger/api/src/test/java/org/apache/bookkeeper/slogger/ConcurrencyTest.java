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
package org.apache.bookkeeper.slogger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Test concurrent access to slogger.
 */
public class ConcurrencyTest {
    enum Events {
        FOOBAR
    }

    @Test
    public void testConcurrentFlattening() throws Exception {
        final int numThreads = 100;
        final int numIterations = 10000;

        Slogger slog = new AbstractSlogger(Collections.emptyList()) {
                @Override
                public Slogger newSlogger(Optional<Class<?>> clazz, Iterable<Object> parent) {
                    return this;
                }
                @Override
                public void doLog(Level level, Enum<?> event, String message,
                                  Throwable throwable, List<Object> keyValues) {
                    for (int i = 0; i < keyValues.size(); i += 2) {
                        if (!keyValues.get(i).equals(keyValues.get(i + 1))) {

                            throw new RuntimeException("Concurrency error");
                        }
                    }
                }
            };

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            futures.add(executor.submit(() -> {
                        for (int j = 0; j < numIterations; j++) {
                            String value = "kv" + Thread.currentThread().getId() + "-" + j;

                            slog.kv(value, value).info(Events.FOOBAR);
                        }
                    }));
        }

        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
    }
}
