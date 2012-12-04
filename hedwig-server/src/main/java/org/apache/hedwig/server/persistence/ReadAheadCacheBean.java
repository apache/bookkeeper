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

package org.apache.hedwig.server.persistence;

import org.apache.hedwig.server.jmx.HedwigMBeanInfo;

/**
 * Read Ahead Cache Bean
 */
public class ReadAheadCacheBean implements ReadAheadCacheMXBean,
        HedwigMBeanInfo {

    ReadAheadCache cache;
    public ReadAheadCacheBean(ReadAheadCache cache) {
        this.cache = cache;
    }

    @Override
    public String getName() {
        return "ReadAheadCache";
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public long getMaxCacheSize() {
        return cache.cfg.getMaximumCacheSize();
    }

    @Override
    public long getPresentCacheSize() {
        return cache.presentCacheSize.get();
    }

    @Override
    public int getNumCachedEntries() {
        return cache.cache.size();
    }

    @Override
    public int getNumPendingCacheRequests() {
        return cache.requestQueue.size();
    }

}
