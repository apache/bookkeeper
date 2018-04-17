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
package org.apache.bookkeeper.versioning;

import lombok.EqualsAndHashCode;

/**
 * A version object holds integer version.
 */
@EqualsAndHashCode
public class LongVersion implements Version {
    protected long version;

    public LongVersion(long v) {
        this.version = v;
    }

    @Override
    public Occurred compare(Version v) {
        if (null == v) {
            throw new NullPointerException("Version is not allowed to be null.");
        }
        if (v == Version.NEW) {
            return Occurred.AFTER;
        } else if (v == Version.ANY) {
            return Occurred.CONCURRENTLY;
        } else if (!(v instanceof LongVersion)) {
            throw new IllegalArgumentException("Invalid version type");
        }
        LongVersion zv = (LongVersion) v;
        int res = Long.compare(version, zv.version);
        if (res == 0) {
            return Occurred.CONCURRENTLY;
        } else if (res < 0) {
            return Occurred.BEFORE;
        } else {
            return Occurred.AFTER;
        }
    }

    public long getLongVersion() {
        return version;
    }

    public LongVersion setLongVersion(long v) {
        this.version = v;
        return this;
    }

    @Override
    public String toString() {
        return Long.toString(version, 10);
    }
}
