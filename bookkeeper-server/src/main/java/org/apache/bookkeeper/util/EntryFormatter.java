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

import java.io.IOException;

import org.apache.commons.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formatter to format an entry
 */
public abstract class EntryFormatter {

    static Logger LOG = LoggerFactory.getLogger(EntryFormatter.class);

    protected Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Format an entry into a readable format
     *
     * @param data
     *          Data Payload
     */
    public abstract void formatEntry(byte[] data);

    /**
     * Format an entry from a string into a readable format
     *
     * @param input
     *          Input Stream
     */
    public abstract void formatEntry(java.io.InputStream input);

    public final static EntryFormatter STRING_FORMATTER = new StringEntryFormatter();

    public static EntryFormatter newEntryFormatter(Configuration conf, String clsProperty) {
        String cls = conf.getString(clsProperty, StringEntryFormatter.class.getName());
        ClassLoader classLoader = EntryFormatter.class.getClassLoader();
        EntryFormatter formatter;
        try {
            Class aCls = classLoader.loadClass(cls);
            formatter = (EntryFormatter) aCls.newInstance();
            formatter.setConf(conf);
        } catch (Exception e) {
            LOG.warn("No formatter class found : " + cls, e);
            LOG.warn("Using Default String Formatter.");
            formatter = STRING_FORMATTER;
        }
        return formatter;
    }
}
