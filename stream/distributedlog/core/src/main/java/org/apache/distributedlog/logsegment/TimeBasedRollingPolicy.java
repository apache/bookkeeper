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
package org.apache.distributedlog.logsegment;

import org.apache.distributedlog.common.util.Sizable;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * TimeBased Policy for rolling.
 *
 */
public class TimeBasedRollingPolicy implements RollingPolicy {

    static final  Logger LOG = LoggerFactory.getLogger(TimeBasedRollingPolicy.class);

    final long rollingIntervalMs;

    public TimeBasedRollingPolicy(long rollingIntervalMs) {
        this.rollingIntervalMs = rollingIntervalMs;
    }

    @Override
    public boolean shouldRollover(Sizable sizable, long lastRolloverTimeMs) {
        long elapsedMs = Utils.elapsedMSec(lastRolloverTimeMs);
        boolean shouldSwitch = elapsedMs > rollingIntervalMs;
        if (shouldSwitch) {
            LOG.debug("Last Finalize Time: {} elapsed time (MSec): {}", lastRolloverTimeMs,
                      elapsedMs);
        }
        return shouldSwitch;
    }

}
