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

import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatter to format an entry.
 */
public abstract class EntryFormatter {

    private static final Logger LOG = LoggerFactory.getLogger(EntryFormatter.class);

    /**
     * Format an entry into a readable format.
     *
     * @param data
     *          Data Payload
     */
    public abstract void formatEntry(byte[] data);

    /**
     * Format an entry from a string into a readable format.
     *
     * @param input
     *          Input Stream
     */
    public abstract void formatEntry(java.io.InputStream input);
    public static final EntryFormatter STRING_FORMATTER = new StringEntryFormatter();

    public static EntryFormatter newEntryFormatter(AbstractConfiguration<?> conf) {
        EntryFormatter formatter;
        try {
            Class<? extends EntryFormatter> entryFormatterClass = conf.getEntryFormatterClass();
            formatter = ReflectionUtils.newInstance(entryFormatterClass);
        } catch (Exception e) {
            LOG.warn("No formatter class found", e);
            LOG.warn("Using Default String Formatter.");
            formatter = new StringEntryFormatter();
        }
        return formatter;
    }

    public static EntryFormatter newEntryFormatter(String opt, AbstractConfiguration conf) {
        EntryFormatter formatter;
        if ("hex".equals(opt)) {
            formatter = new HexDumpEntryFormatter();
        } else if ("string".equals(opt)) {
            formatter = new StringEntryFormatter();
        } else {
            LOG.warn("specified unexpected entryformat {}, so default EntryFormatter is used", opt);
            formatter = newEntryFormatter(conf);
        }
        return formatter;
    }
}
