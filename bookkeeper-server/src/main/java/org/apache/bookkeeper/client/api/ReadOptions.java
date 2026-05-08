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
package org.apache.bookkeeper.client.api;

import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;

/**
 * Options that apply to a single read request.
 */
@Public
@Unstable
public final class ReadOptions {

    /**
     * Default read options. Read-ahead behavior is unchanged.
     */
    public static final ReadOptions DEFAULT = builder().build();

    private final boolean disableReadAhead;

    private ReadOptions(boolean disableReadAhead) {
        this.disableReadAhead = disableReadAhead;
    }

    /**
     * Whether this read should avoid triggering bookie-side read-ahead on cache miss.
     *
     * <p>This option does not bypass existing cache entries.
     */
    public boolean isReadAheadDisabled() {
        return disableReadAhead;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean disableReadAhead;

        private Builder() {
        }

        public Builder disableReadAhead(boolean disableReadAhead) {
            this.disableReadAhead = disableReadAhead;
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(disableReadAhead);
        }
    }
}
