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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Abstract implementation of slogger. Keeps track of key value pairs.
 */
public abstract class AbstractSlogger implements Slogger, Iterable<Object> {
    /**
     * Levels at which slogger can slog.
     */
    public enum Level {
        INFO,
        WARN,
        ERROR
    }

    private static final int MAX_DEPTH = 3;
    private List<String> parentCtx;

    private ThreadLocal<List<Object>> kvs = new ThreadLocal<List<Object>>() {
            @Override
            protected List<Object> initialValue() {
                return new ArrayList<>();
            }
        };
    private ThreadLocal<List<Object>> flattenedTls = ThreadLocal.withInitial(ArrayList::new);

    protected AbstractSlogger(Iterable<Object> parentCtx) {
        List<String> flattened = new ArrayList<>();
        flattenKeyValues(parentCtx.iterator(), (k, v) -> {
                flattened.add(k);
                flattened.add(v);
            });
        this.parentCtx = Collections.unmodifiableList(flattened);
    }

    protected abstract Slogger newSlogger(Optional<Class<?>> clazz, Iterable<Object> parent);
    protected abstract void doLog(Level level, Enum<?> event, String message,
                                  Throwable throwable, List<Object> keyValues);

    private void flattenAndLog(Level level, Enum<?> event, String message,
                               Throwable throwable) {
        List<Object> flattened = flattenedTls.get();
        flattened.clear();

        flattenKeyValues(this::addToFlattened);
        doLog(level, event, message, throwable, flattened);
    }

    @Override
    public void info(String message) {
        flattenAndLog(Level.INFO, null, message, null);
    }

    @Override
    public void info(String message, Throwable cause) {
        flattenAndLog(Level.INFO, null, message, cause);
    }

    @Override
    public void info(Enum<?> event) {
        flattenAndLog(Level.INFO, event, null, null);
    }

    @Override
    public void info(Enum<?> event, Throwable cause) {
        flattenAndLog(Level.INFO, event, null, cause);
    }

    @Override
    public void warn(String message) {
        flattenAndLog(Level.WARN, null, message, null);
    }

    @Override
    public void warn(String message, Throwable cause) {
        flattenAndLog(Level.WARN, null, message, cause);
    }

    @Override
    public void warn(Enum<?> event) {
        flattenAndLog(Level.WARN, event, null, null);
    }

    @Override
    public void warn(Enum<?> event, Throwable cause) {
        flattenAndLog(Level.WARN, event, null, cause);
    }

    @Override
    public void error(String message) {
        flattenAndLog(Level.ERROR, null, message, null);
    }

    @Override
    public void error(String message, Throwable cause) {
        flattenAndLog(Level.ERROR, null, message, cause);
    }

    @Override
    public void error(Enum<?> event) {
        flattenAndLog(Level.ERROR, event, null, null);
    }

    @Override
    public void error(Enum<?> event, Throwable cause) {
        flattenAndLog(Level.ERROR, event, null, cause);
    }

    @Override
    public Slogger ctx() {
        try {
            return newSlogger(Optional.empty(), this);
        } finally {
            kvs.get().clear();
        }
    }

    @Override
    public Slogger ctx(Class<?> clazz) {
        try {
            return newSlogger(Optional.of(clazz), this);
        } finally {
            kvs.get().clear();
        }
    }

    @Override
    public Iterator<Object> iterator() {
        CtxIterator iterator = this.iterator.get();
        iterator.reset();
        return iterator;
    }

    protected void clearCurrentCtx() {
        kvs.get().clear();
    }

    private void addToFlattened(String key, String value) {
        flattenedTls.get().add(key);
        flattenedTls.get().add(value);
    }

    protected void flattenKeyValues(BiConsumer<String, String> consumer) {
        Iterator<Object> iter = iterator();
        try {
            flattenKeyValues(iter, consumer);
        } finally {
            kvs.get().clear();
        }
    }

    public static void flattenKeyValues(Iterator<Object> iter,
                                        BiConsumer<String, String> consumer) {
        while (iter.hasNext()) {
            String key = iter.next().toString();
            if (!iter.hasNext()) {
                return; // key without value
            }
            Object value = iter.next();

            if (value instanceof Sloggable) {
                addWithPrefix(key, (Sloggable) value, consumer, 0);
            } else if (value.getClass().isArray()) {
                consumer.accept(key, arrayToString(value));
            } else {
                consumer.accept(key, value.toString());
            }
        }
    }

    @Override
    public Slogger kv(Object key, Object value) {
        kvs.get().add(key);
        kvs.get().add(value);
        return this;
    }

    private static void addWithPrefix(String prefix, Sloggable value,
                                      BiConsumer<String, String> consumer, int depth) {
        value.log(new SloggableAccumulator() {
                @Override
                public SloggableAccumulator kv(Object key, Object value) {
                    if (value instanceof Sloggable && depth < MAX_DEPTH) {
                        addWithPrefix(prefix + "." + key.toString(),
                                      (Sloggable) value, consumer, depth + 1);
                    } else if (value.getClass().isArray()) {
                        consumer.accept(prefix + "." + key.toString(), arrayToString(value));
                    } else {
                        consumer.accept(prefix + "." + key.toString(), value.toString());
                    }
                    return this;
                }
            });
    }

    private static String arrayToString(Object o) {
        if (o instanceof long[]) {
            return Arrays.toString((long[]) o);
        } else if (o instanceof int[]) {
            return Arrays.toString((int[]) o);
        } else if (o instanceof short[]) {
            return Arrays.toString((short[]) o);
        } else if (o instanceof char[]) {
            return Arrays.toString((char[]) o);
        } else if (o instanceof byte[]) {
            return Arrays.toString((byte[]) o);
        } else if (o instanceof boolean[]) {
            return Arrays.toString((boolean[]) o);
        } else if (o instanceof float[]) {
            return Arrays.toString((float[]) o);
        } else if (o instanceof double[]) {
            return Arrays.toString((double[]) o);
        } else if (o instanceof Object[]) {
            return Arrays.toString((Object[]) o);
        } else {
            return o.toString();
        }
    }

    private final ThreadLocal<CtxIterator> iterator = new ThreadLocal<CtxIterator>() {
            @Override
            protected CtxIterator initialValue() {
                return new CtxIterator();
            }
        };
    class CtxIterator implements Iterator<Object> {
        int index = 0;

        private void reset() {
            index = 0;
        }

        @Override
        public boolean hasNext() {
            return index < (parentCtx.size() + kvs.get().size());
        }

        @Override
        public Object next() {
            int i = index++;
            if (i < parentCtx.size()) {
                return parentCtx.get(i);
            } else {
                i -= parentCtx.size();
                return kvs.get().get(i);
            }
        }
    }
}
