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

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Simple slogger implementation which writes json to console.
 */
public class ConsoleSlogger extends AbstractSlogger {
    private static final int MAX_STACKTRACE_ELEMENTS = 20;
    private static final int MAX_CAUSES = 10;
    private final Class<?> clazz;

    ConsoleSlogger() {
        this(ConsoleSlogger.class);
    }

    ConsoleSlogger(Class<?> clazz) {
        this(clazz, Collections.emptyList());
    }

    ConsoleSlogger(Class<?> clazz, Iterable<Object> parent) {
        super(parent);
        this.clazz = clazz;
    }

    @Override
    protected Slogger newSlogger(Optional<Class<?>> clazz, Iterable<Object> parent) {
        return new ConsoleSlogger(clazz.orElse(ConsoleSlogger.class), parent);
    }

    @Override
    protected void doLog(Level level, Enum<?> event, String message,
                         Throwable throwable, List<Object> keyValues) {
        String nowAsISO = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);

        StringBuilder builder = new StringBuilder();
        builder.append("{");
        keyValue(builder, "date", nowAsISO);
        builder.append(",");
        keyValue(builder, "level", level.toString());
        if (event != null) {
            builder.append(",");
            keyValue(builder, "event", event.toString());
        }
        if (message != null) {
            builder.append(",");
            keyValue(builder, "message", message);
        }

        for (int i = 0; i < keyValues.size(); i += 2) {
            builder.append(",");
            keyValue(builder, keyValues.get(i).toString(), keyValues.get(i + 1).toString());
        }
        if (throwable != null) {
            builder.append(",");
            Throwable cause = throwable;
            StringBuilder stacktrace = new StringBuilder();
            int causes = 0;
            while (cause != null) {
                stacktrace.append("[").append(cause.getMessage()).append("] at ");
                int i = 0;
                for (StackTraceElement element : cause.getStackTrace()) {
                    if (i++ > MAX_STACKTRACE_ELEMENTS) {
                        stacktrace.append("<|[frames omitted]");
                    }
                    stacktrace.append("<|").append(element.toString());
                }
                cause = cause.getCause();
                if (cause != null) {
                    if (causes++ > MAX_CAUSES) {
                        stacktrace.append(" [max causes exceeded] ");
                        break;
                    } else {
                        stacktrace.append(" caused by ");
                    }
                }
            }
            keyValue(builder, "exception", stacktrace.toString());
        }
        builder.append("}");

        System.out.println(builder.toString());
    }

    private static void keyValue(StringBuilder sb, String key, String value) {
        quotedAppend(sb, key);
        sb.append(":");
        quotedAppend(sb, value);
    }

    private static void quotedAppend(StringBuilder sb, String str) {
        sb.append('"');
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (c == '\\') {
                sb.append("\\\\");
            } else if (c == '"') {
                sb.append("\\\"");
            } else if (c < ' ') {
                sb.append(String.format("\\u%04X", (int) c));
            } else {
                sb.append(c);
            }
        }
        sb.append('"');
    }
}
