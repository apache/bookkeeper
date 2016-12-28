/**
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
package com.twitter.distributedlog.logsegment;

import com.google.common.annotations.Beta;
import com.twitter.distributedlog.LogSegmentMetadata;
import com.twitter.util.Future;

/**
 * Log Segment Store to read log segments
 */
@Beta
public interface LogSegmentEntryStore {

    /**
     * Open the writer for writing data to the log <i>segment</i>.
     *
     * @param segment the log <i>segment</i> to write data to
     * @return future represent the opened writer
     */
    Future<LogSegmentEntryWriter> openWriter(LogSegmentMetadata segment);

    /**
     * Open the reader for reading data to the log <i>segment</i>.
     *
     * @param segment the log <i>segment</i> to read data from
     * @return future represent the opened reader
     */
    Future<LogSegmentEntryReader> openReader(LogSegmentMetadata segment);

}
