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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
/**
 * Mock Slogger.
 */
public class MockSlogger extends AbstractSlogger {
    List<MockEvent> events = new ArrayList<>();

    public MockSlogger() {
        super(new ArrayList<>());
    }

    private MockSlogger(Iterable<Object> parentCtx) {
        super(parentCtx);
    }

    @Override
    protected Slogger newSlogger(Optional<Class<?>> clazz, Iterable<Object> parentCtx) {
        return new MockSlogger(parentCtx);
    }

    @Override
    protected void doLog(Level level, Enum<?> event, String message, Throwable throwable,
                         List<Object> keyValues) {
        Map<String, Object> tmpKvs = new HashMap<>();
        for (int i = 0; i < keyValues.size(); i += 2) {
            tmpKvs.put(keyValues.get(i).toString(), keyValues.get(i + 1));
        }
        events.add(new MockEvent(level, event, message, tmpKvs, throwable));
    }

    static class MockEvent {
        private final Level level;
        private final Enum<?> event;
        private final String message;
        private final Map<String, Object> kvs;
        private final Throwable throwable;

        MockEvent(Level level, Enum<?> event, String message,
                  Map<String, Object> kvs, Throwable throwable) {
            this.level = level;
            this.event = event;
            this.message = message;
            this.kvs = kvs;
            this.throwable = throwable;
        }

        Level getLevel() {
            return level;
        }
        Enum<?> getEvent() {
            return event;
        }
        String getMessage() {
            return message;
        }
        Map<String, Object> getKeyValues() {
            return kvs;
        }
        Throwable getThrowable() {
            return throwable;
        }
    }
}
