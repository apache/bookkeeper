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

package org.apache.bookkeeper.bookie;

import java.util.concurrent.Future;
/**
 * State management of Bookie, including register, turn bookie to w/r mode.
 */
public interface StateManager extends AutoCloseable {

    /**
     * Init state of Bookie when launch bookie.
     */
    void initState();

    /**
     * Check if the bookie is available for high priority writes or not.
     *
     * @return true if the bookie is available for high priority writes; otherwise false.
     */
    boolean isAvailableForHighPriorityWrites();

    /**
     * Enable/Disable the availability for high priority writes.
     *
     * @param available the flag to enable/disable the availability for high priority writes.
     */
    void setHighPriorityWritesAvailability(boolean available);

    /**
     * Check is ReadOnly.
     */
    boolean isReadOnly();

    /**
     * Check is forceReadOnly.
     */
    boolean isForceReadOnly();

    /**
     * Check is Running.
     */
    boolean isRunning();

    /**
     * Check is Shutting down.
     */
    boolean isShuttingDown();

    /**
     * Close the manager, release its resources.
     */
    @Override
    void close();

    /**
     * Register the bookie to RegistrationManager.
     * @params throwException, whether throwException or not
     */
    Future<Void> registerBookie(boolean throwException);

    // forceTos methods below should be called inside Bookie,
    // which indicates important state of bookie and should be visible fast.
    /**
     * Turn state to the shutting down progress,just the flag.
     */
    void forceToShuttingDown();

    /**
     * Turn state to the read only, just flag.
     */
    void forceToReadOnly();

    /**
     * Turn state to not registered, just the flag.
     */
    void forceToUnregistered();

    /**
     * Change the state of bookie to Writable mode.
     */
    Future<Void> transitionToWritableMode();

    /**
     * Change the state of bookie to ReadOnly mode.
     */
    Future<Void> transitionToReadOnlyMode();

    /**
     * ShutdownHandler used to shutdown bookie.
     */
    interface ShutdownHandler {
        void shutdown(int code);
    }

    void setShutdownHandler(ShutdownHandler handler);
}

