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

package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.jmx.BKMBeanInfo;

/**
 * Ledger Cache Bean
 */
public class LedgerCacheBean implements LedgerCacheMXBean, BKMBeanInfo {

    final LedgerCache lc;

    public LedgerCacheBean(LedgerCache lc) {
        this.lc = lc;
    }

    @Override
    public String getName() {
        return "LedgerCache";
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public int getPageCount() {
        return lc.getNumUsedPages();
    }

    @Override
    public int getPageSize() {
        return lc.getPageSize();
    }

    @Override
    public int getOpenFileLimit() {
        return lc.openFileLimit;
    }

    @Override
    public int getPageLimit() {
        return lc.getPageLimit();
    }

    @Override
    public int getNumCleanLedgers() {
        return lc.cleanLedgers.size();
    }

    @Override
    public int getNumDirtyLedgers() {
        return lc.dirtyLedgers.size();
    }

    @Override
    public int getNumOpenLedgers() {
        return lc.openLedgers.size();
    }

}
