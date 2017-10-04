/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.distributedlog.api;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.io.Abortable;


/**
 * A generic interface class to support writing log records into
 * a persistent distributed log.
 */
@Public
@Evolving
public interface LogWriter extends Closeable, Abortable {
    /**
     * Write a log record to the stream.
     *
     * @param record single log record
     * @throws IOException
     */
    void write(LogRecord record) throws IOException;


    /**
     * Write a list of log records to the stream.
     *
     * @param records list of log records
     * @throws IOException
     */
    @Deprecated
    int writeBulk(List<LogRecord> records) throws IOException;

    /**
     * All data that has been written to the stream so far will be sent to
     * persistent storage.
     * The transmission is asynchronous and new data can be still written to the
     * stream while flushing is performed.
     */
    long flush() throws IOException;

    /**
     * Flush and sync all data that is ready to be flush
     * {@link #flush()} into underlying persistent store.
     * @throws IOException
     */
    long commit() throws IOException;

    /**
     * Flushes all the data up to this point,
     * adds the end of stream marker and marks the stream
     * as read-only in the metadata. No appends to the
     * stream will be allowed after this point
     *
     * @throws IOException
     */
    void markEndOfStream() throws IOException;

}
