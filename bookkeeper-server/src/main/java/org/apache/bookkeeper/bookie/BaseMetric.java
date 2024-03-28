/**
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

package org.apache.bookkeeper.bookie;

public class BaseMetric {
    int journalIoUtil;
    int ledgerIoUtil;
    int cpuUsedRate;
    long writeBytePerSecond;

    BaseMetric() {
        journalIoUtil = -1;
        ledgerIoUtil = -1;
        cpuUsedRate = -1;
        writeBytePerSecond = -1;
    }

    public int getJournalIoUtil() {
        return journalIoUtil;
    }

    public void setJournalIoUtil(int journalIoUtil) {
        this.journalIoUtil = journalIoUtil;
    }

    public int getLedgerIoUtil() {
        return ledgerIoUtil;
    }

    public void setLedgerIoUtil(int ledgerIoUtil) {
        this.ledgerIoUtil = ledgerIoUtil;
    }

    public int getCpuUsedRate() {
        return cpuUsedRate;
    }

    public void setCpuUsedRate(int cpuUsedRate) {
        this.cpuUsedRate = cpuUsedRate;
    }

    public long getWriteBytePerSecond() {
        return writeBytePerSecond;
    }

    public void setWriteBytePerSecond(long writeBytePerSecond) {
        this.writeBytePerSecond = writeBytePerSecond;
    }

    @Override
    public String toString() {
        return "BaseMetric{" +
            "journalIoUtil=" + journalIoUtil +
            ", ledgerIoUtil=" + ledgerIoUtil +
            ", cpuUsedRate=" + cpuUsedRate +
            ", writeBytePerSecond=" + writeBytePerSecond +
            '}';
    }
}
