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
package org.apache.bookkeeper.slogger.slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.bookkeeper.slogger.AbstractSlogger;
import org.apache.bookkeeper.slogger.Slogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Slf4j implementation of slogger.
 */
public class Slf4jSlogger extends AbstractSlogger {
    private ThreadLocal<List<String>> mdcKeysTls = new ThreadLocal<List<String>>() {
            @Override
            protected List<String> initialValue() {
                return new ArrayList<>();
            }
        };

    private final Logger log;

    public Slf4jSlogger(Class<?> clazz) {
        this(clazz, Collections.emptyList());
    }

    Slf4jSlogger() {
        this(Slf4jSlogger.class);
    }

    Slf4jSlogger(Class<?> clazz, Iterable<Object> parent) {
        super(parent);
        this.log = LoggerFactory.getLogger(clazz);
    }

    @Override
    protected Slogger newSlogger(Optional<Class<?>> clazz, Iterable<Object> parent) {
        return new Slf4jSlogger(clazz.orElse(Slf4jSlogger.class), parent);
    }

    @Override
    protected void doLog(Level level, Enum<?> event, String message,
                         Throwable throwable, List<Object> keyValues) {
        List<String> mdcKeys = mdcKeysTls.get();
        mdcKeys.clear();
        try {
            if (event != null) {
                MDC.put("event", event.toString());
                mdcKeys.add("event");
            }

            for (int i = 0; i < keyValues.size(); i += 2) {
                MDC.put(keyValues.get(i).toString(), keyValues.get(i + 1).toString());
                mdcKeys.add(keyValues.get(i).toString());
            }

            String msg = message == null ? event.toString() : message;
            switch (level) {
            case INFO:
                log.info(msg);
                break;
            case WARN:
                if (throwable != null) {
                    log.warn(msg, throwable);
                } else {
                    log.warn(msg);
                }
                break;
            default:
            case ERROR:
                if (throwable != null) {
                    log.error(msg, throwable);
                } else {
                    log.error(msg);
                }
                break;
            }
        } finally {
            for (String key : mdcKeys) {
                MDC.remove(key);
            }
            mdcKeys.clear();
        }
    }
}
