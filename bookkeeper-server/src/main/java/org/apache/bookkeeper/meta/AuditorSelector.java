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

package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.concurrent.Future;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;

/**
 * Interface for selecting an auditor.
 */
public interface AuditorSelector extends AutoCloseable {

    /**
     * Selector listener to listen on auditor changes.
     */
    interface SelectorListener {

        /**
         * Trigger when it is selected as a leader.
         */
        void onLeaderSelected() throws IOException;

        /**
         * Trigger on each selection attempt.
         */
        void onSelectionAttempt();

        /**
         * Trigger when it lost its leadership.
         */
        void onLeaderExpired();

    }

    /**
     * Whether the selector is running or not.
     *
     * @return true if the selector is running, otherwise false.
     */
    boolean isRunning();

    /**
     * Trigger the selecting process with the provided <tt>listener</tt>.
     *
     * @param listener the listener to listen on events triggered by selection.
     * @return a future when the selector is running
     */
    Future<?> select(SelectorListener listener)
        throws MetadataException;

    /**
     * Get the current auditor.
     *
     * @return current auditor.
     */
    BookieSocketAddress getCurrentAuditor() throws MetadataException;

    @Override
    void close();
}
