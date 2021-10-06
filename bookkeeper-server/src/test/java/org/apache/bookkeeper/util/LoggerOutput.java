/*
 *
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
 *
 */
package org.apache.bookkeeper.util;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.NullAppender;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;
import org.slf4j.Marker;
import org.slf4j.event.Level;
import org.slf4j.event.LoggingEvent;

/**
 * A utility class for testing logger output.
 */
public class LoggerOutput implements TestRule {

    private NullAppender logAppender;
    private ArgumentCaptor<LogEvent> logEventCaptor;
    private final List<Consumer<List<LoggingEvent>>> logEventExpectations = new ArrayList<>();

    public void expect(Consumer<List<LoggingEvent>> expectation) {
        if (logEventCaptor == null) {
            logEventCaptor = ArgumentCaptor.forClass(LogEvent.class);
        }
        logEventExpectations.add(expectation);
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                LoggerContext lc = (LoggerContext) LogManager.getContext(false);
                logAppender = spy(NullAppender.createAppender(UUID.randomUUID().toString()));
                logAppender.start();
                lc.getConfiguration().addAppender(logAppender);
                lc.getRootLogger().addAppender(lc.getConfiguration().getAppender(logAppender.getName()));
                lc.updateLoggers();
                try {
                    base.evaluate();
                    if (!logEventExpectations.isEmpty()) {
                        verify(logAppender, atLeastOnce()).append(logEventCaptor.capture());
                        List<LoggingEvent> logEvents = logEventCaptor.getAllValues().stream()
                                .map(LoggerOutput::toSlf4j)
                                .collect(Collectors.toList());
                        for (Consumer<List<LoggingEvent>> expectation : logEventExpectations) {
                            expectation.accept(logEvents);
                        }
                    }
                } finally {
                    lc.getRootLogger().removeAppender(lc.getConfiguration().getAppender(logAppender.getName()));
                    lc.updateLoggers();
                    logEventExpectations.clear();
                    logEventCaptor = null;
                }
            }
        };
    }

    private static LoggingEvent toSlf4j(LogEvent log4jEvent) {
        return new LoggingEvent() {
            @Override
            public Level getLevel() {
                switch (log4jEvent.getLevel().toString()) {
                    case "FATAL":
                    case "ERROR": return Level.ERROR;
                    case "WARN": return Level.WARN;
                    case "INFO": return Level.INFO;
                    case "DEBUG": return Level.DEBUG;
                    case "TRACE":
                    case "ALL":
                    case "OFF":
                    default: return Level.TRACE;
                }
            }

            @Override
            public Marker getMarker() {
                return null;
            }

            @Override
            public String getLoggerName() {
                return log4jEvent.getLoggerName();
            }

            @Override
            public String getMessage() {
                return log4jEvent.getMessage().getFormattedMessage();
            }

            @Override
            public String getThreadName() {
                return log4jEvent.getThreadName();
            }

            @Override
            public Object[] getArgumentArray() {
                return new Object[0];
            }

            @Override
            public long getTimeStamp() {
                return log4jEvent.getTimeMillis();
            }

            @Override
            public Throwable getThrowable() {
                return null;
            }
        };
    }
}
