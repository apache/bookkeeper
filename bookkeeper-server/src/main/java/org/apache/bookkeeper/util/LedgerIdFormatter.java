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

import java.util.UUID;

import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatter to format a ledgerId.
 */
public abstract class LedgerIdFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(LedgerIdFormatter.class);

    /**
     * Formats the LedgerId according to the type of the Formatter and return it
     * in String format.
     *
     * @param ledgerId
     * @return
     */
    public abstract String formatLedgerId(long ledgerId);

    /**
     * converts the ledgeridString, which is in format of the type of formatter,
     * to the long value.
     *
     * @param ledgerIdString
     * @return
     */
    public abstract long readLedgerId(String ledgerIdString);

    // Used by BKExtentIdByteArray
    public static final LedgerIdFormatter LONG_LEDGERID_FORMATTER = new LongLedgerIdFormatter();

    public static LedgerIdFormatter newLedgerIdFormatter(AbstractConfiguration<?> conf) {
        LedgerIdFormatter formatter;
        try {
            Class<? extends LedgerIdFormatter> ledgerIdFormatterClass = conf.getLedgerIdFormatterClass();
            formatter = ReflectionUtils.newInstance(ledgerIdFormatterClass);
        } catch (Exception e) {
            LOG.warn("No formatter class found", e);
            LOG.warn("Using Default Long Formatter.");
            formatter = new LongLedgerIdFormatter();
        }
        return formatter;
    }

    public static LedgerIdFormatter newLedgerIdFormatter(String opt, AbstractConfiguration conf) {
        LedgerIdFormatter formatter;
        if ("hex".equals(opt)) {
            formatter = new LedgerIdFormatter.HexLedgerIdFormatter();
        } else if ("uuid".equals(opt)) {
            formatter = new LedgerIdFormatter.UUIDLedgerIdFormatter();
        } else if ("long".equals(opt)) {
            formatter = new LedgerIdFormatter.LongLedgerIdFormatter();
        } else {
            LOG.warn("specified unexpected ledgeridformat {}, so default LedgerIdFormatter is used", opt);
            formatter = newLedgerIdFormatter(conf);
        }
        return formatter;
    }

    /**
     * long ledgerId formatter.
     */
    public static class LongLedgerIdFormatter extends LedgerIdFormatter {

        @Override
        public String formatLedgerId(long ledgerId) {
            return Long.toString(ledgerId);
        }

        @Override
        public long readLedgerId(String ledgerIdString) {
            return Long.parseLong(ledgerIdString.trim());
        }
    }

    /**
     * hex ledgerId formatter.
     */
    public static class HexLedgerIdFormatter extends LedgerIdFormatter {

        @Override
        public String formatLedgerId(long ledgerId) {
            return Long.toHexString(ledgerId);
        }

        @Override
        public long readLedgerId(String ledgerIdString) {
            return Long.valueOf(ledgerIdString.trim(), 16);
        }
    }

    /**
     * uuid ledgerId formatter.
     */
    public static class UUIDLedgerIdFormatter extends LedgerIdFormatter {

        @Override
        public String formatLedgerId(long ledgerId) {
            return (new UUID(0, ledgerId)).toString();
        }

        @Override
        public long readLedgerId(String ledgerIdString) {
            return UUID.fromString(ledgerIdString.trim()).getLeastSignificantBits();
        }
    }
}
