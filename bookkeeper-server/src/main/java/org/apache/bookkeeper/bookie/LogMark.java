/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.bookkeeper.bookie;

import java.nio.ByteBuffer;

/**
 * Journal stream position.
 */
public class LogMark {
    long logFileId;
    long logFileOffset;

    public static final LogMark MAX_VALUE = new LogMark(Long.MAX_VALUE, Long.MAX_VALUE);

    public LogMark() {
        setLogMark(0, 0);
    }

    public LogMark(LogMark other) {
        setLogMark(other.getLogFileId(), other.getLogFileOffset());
    }

    public LogMark(long logFileId, long logFileOffset) {
        setLogMark(logFileId, logFileOffset);
    }

    public synchronized long getLogFileId() {
        return logFileId;
    }

    public synchronized long getLogFileOffset() {
        return logFileOffset;
    }

    public synchronized void readLogMark(ByteBuffer bb) {
        logFileId = bb.getLong();
        logFileOffset = bb.getLong();
    }

    public synchronized void writeLogMark(ByteBuffer bb) {
        bb.putLong(logFileId);
        bb.putLong(logFileOffset);
    }

    public synchronized void setLogMark(long logFileId, long logFileOffset) {
        this.logFileId = logFileId;
        this.logFileOffset = logFileOffset;
    }

    public synchronized int compare(LogMark other) {
        long ret = this.logFileId - other.getLogFileId();
        if (ret == 0) {
            ret = this.logFileOffset - other.getLogFileOffset();
        }
        return (ret < 0) ? -1 : ((ret > 0) ? 1 : 0);
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("LogMark: logFileId - ").append(logFileId)
                .append(" , logFileOffset - ").append(logFileOffset);

        return sb.toString();
    }
}
