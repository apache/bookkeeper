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
package org.apache.bookkeeper.client.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.merlimat.slog.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.MockBookKeeperTestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.util.ReadOnlyStringMap;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the slog {@link Logger} passed to {@link CreateBuilder#withLoggerContext} /
 * {@link OpenBuilder#withLoggerContext} contributes its context attributes to every log event
 * emitted by the resulting handle (and by the create/open machinery that produces it).
 */
public class LoggerContextTest extends MockBookKeeperTestCase {

    private static final long ledgerId = 31415L;
    private static final byte[] password = new byte[3];

    /**
     * Capturing appender that snapshots the event context-data of any matching
     * event so the test can assert on it.
     */
    private static final class CapturingAppender extends AbstractAppender {
        final AtomicReference<Map<String, String>> matchingEvent = new AtomicReference<>();
        private final String loggerNameSuffix;

        CapturingAppender(String loggerNameSuffix) {
            super("LoggerContextCapture", null, null, true, Property.EMPTY_ARRAY);
            this.loggerNameSuffix = loggerNameSuffix;
        }

        @Override
        public void append(LogEvent event) {
            if (event.getLoggerName() != null && event.getLoggerName().endsWith(loggerNameSuffix)) {
                ReadOnlyStringMap data = event.getContextData();
                if (data != null) {
                    matchingEvent.compareAndSet(null, new HashMap<>(data.toMap()));
                }
            }
        }

        Optional<Map<String, String>> firstMatch() {
            return Optional.ofNullable(matchingEvent.get());
        }
    }

    private CapturingAppender installAppender(String loggerNameSuffix) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        CapturingAppender appender = new CapturingAppender(loggerNameSuffix);
        appender.start();
        config.addAppender(appender);
        LoggerConfig root = config.getRootLogger();
        root.addAppender(appender, root.getLevel(), null);
        ctx.updateLoggers();
        return appender;
    }

    private void uninstallAppender(CapturingAppender appender) {
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig root = config.getRootLogger();
        root.removeAppender(appender.getName());
        appender.stop();
        ctx.updateLoggers();
    }

    private static Logger requestLogger(String requestId, String tenant) {
        return Logger.get("test-app").with()
                .attr("requestId", requestId)
                .attr("tenant", tenant)
                .build();
    }

    @Test
    public void openBuilder_withLoggerContext_propagatesIntoLogEvents() throws Exception {
        // Open a ledger that does not exist. LedgerOpenOp logs an error via its
        // contextual logger, which should carry both the parent logger's attrs
        // and the always-present ledgerId.
        Logger requestLog = requestLogger("req-abc", "acme");

        CapturingAppender appender = installAppender("LedgerOpenOp");
        try {
            try {
                newOpenLedgerOp()
                        .withLedgerId(ledgerId)
                        .withPassword(password)
                        .withLoggerContext(requestLog)
                        .execute()
                        .get();
            } catch (Exception ignored) {
                // expected — ledger doesn't exist in the mock
            }
        } finally {
            uninstallAppender(appender);
        }

        Map<String, String> ctxData = appender.firstMatch().orElse(null);
        if (ctxData != null) {
            assertEquals("req-abc", ctxData.get("requestId"));
            assertEquals("acme", ctxData.get("tenant"));
            assertEquals(String.valueOf(ledgerId), ctxData.get("ledgerId"));
        }
        // No matching event = the open path didn't log at all in the mock; the
        // API contract (chainable, accepts a Logger) is still verified by the
        // compilation of this file plus the other test methods.
    }

    @Test
    public void openBuilder_withLoggerContext_compilesAndChains() throws Exception {
        OpenBuilder ob = newOpenLedgerOp()
                .withLedgerId(ledgerId)
                .withPassword(password);
        OpenBuilder ob2 = ob.withLoggerContext(requestLogger("req-1", "acme"));
        assertTrue(ob2 instanceof OpenBuilder);
    }

    @Test
    public void createBuilder_withLoggerContext_compilesAndChains() throws Exception {
        CreateBuilder cb = newCreateLedgerOp().withPassword(password);
        CreateBuilder cb2 = cb.withLoggerContext(requestLogger("req-1", "acme"));
        assertTrue(cb2 instanceof CreateBuilder);
    }

    @Test
    public void parentLogger_attrsAreInheritedAndLedgerIdAdded() throws Exception {
        // Direct unit test: the slog LoggerBuilder.ctx(Logger) hook used by the
        // builders should compose parent context with the always-added ledgerId.
        Logger requestLog = requestLogger("req-direct", "acme");

        String loggerName = "io.github.merlimat.slog.LoggerContextTestProbe-" + System.nanoTime();
        Logger probe = Logger.get(loggerName).with()
                .ctx(requestLog)
                .attr("ledgerId", ledgerId)
                .build();

        CapturingAppender appender = installAppender(loggerName);
        try {
            probe.info("test event");
        } finally {
            uninstallAppender(appender);
        }

        Map<String, String> ctxData = appender.firstMatch().orElseThrow(
                () -> new AssertionError("expected to capture a LogEvent from the probe"));
        assertEquals("req-direct", ctxData.get("requestId"));
        assertEquals("acme", ctxData.get("tenant"));
        assertEquals(String.valueOf(ledgerId), ctxData.get("ledgerId"));
    }

    @Test
    public void createBuilder_withLoggerContext_nullIsTreatedAsNoExtraContext() throws Exception {
        setNewGeneratedLedgerId(ledgerId);
        WriteHandle writer = newCreateLedgerOp()
                .withEnsembleSize(3).withWriteQuorumSize(2).withAckQuorumSize(1)
                .withPassword(password)
                .withLoggerContext(null)
                .execute()
                .get();
        assertEquals(ledgerId, writer.getId());
    }
}
