/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.stream.storage.conf;

import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;

/**
 * Storage related configuration settings.
 */
public class StorageConfiguration extends ComponentConfiguration {

    private static final String COMPONENT_PREFIX = "storage" + DELIMITER;

    private static final String RANGE_STORE_DIRS = "range.store.dirs";

    private static final String SERVE_READONLY_TABLES = "serve.readonly.tables";

    private static final String CONTROLLER_SCHEDULE_INTERVAL_MS = "cluster.controller.schedule.interval.ms";

    public StorageConfiguration(CompositeConfiguration conf) {
        super(conf, COMPONENT_PREFIX);
    }

    public StorageConfiguration setRangeStoreDirNames(String[] dirNames) {
        this.setProperty(RANGE_STORE_DIRS, dirNames);
        return this;
    }

    private String[] getRangeStoreDirNames() {
        String[] rangeStoreDirs = getStringArray(RANGE_STORE_DIRS);
        if (null == rangeStoreDirs || 0 == rangeStoreDirs.length) {
            return new String[]{"data/bookkeeper/ranges"};
        } else {
            return rangeStoreDirs;
        }
    }

    public File[] getRangeStoreDirs() {
        String[] rangeStoreDirNames = getRangeStoreDirNames();

        File[] rangeStoreDirs = new File[rangeStoreDirNames.length];
        for (int i = 0; i < rangeStoreDirNames.length; i++) {
            rangeStoreDirs[i] = new File(rangeStoreDirNames[i]);
        }
        return rangeStoreDirs;
    }

    public StorageConfiguration setServeReadOnlyTables(boolean serveReadOnlyTables) {
        this.setProperty(SERVE_READONLY_TABLES, serveReadOnlyTables);
        return this;
    }

    public boolean getServeReadOnlyTables() {
        return getBoolean(SERVE_READONLY_TABLES, false);
    }

    /**
     * Get the cluster controller schedule interval in milliseconds. The default value is 30 seconds.
     *
     * @return cluster controller schedule interval, in milliseconds.
     */
    public long getClusterControllerScheduleIntervalMs() {
        return getLong(CONTROLLER_SCHEDULE_INTERVAL_MS, TimeUnit.SECONDS.toMillis(30));
    }

    /**
     * Set the cluster controller schedule interval.
     *
     * @param time time value
     * @param timeUnit time unit
     * @return storage configuration
     */
    public StorageConfiguration setClusterControllerScheduleInterval(long time, TimeUnit timeUnit) {
        setProperty(CONTROLLER_SCHEDULE_INTERVAL_MS, timeUnit.toMillis(time));
        return this;
    }

}
