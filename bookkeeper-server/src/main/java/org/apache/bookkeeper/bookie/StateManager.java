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
     * Close the manager, release its resources.
     */
    @Override
    void close();

    /**
     * Register the bookie to RegistrationManager.
     * @params throwException, whether throwException or not
     */
    Future<Void> registerBookie(boolean throwException);

    /**
     * Change the state of bookie to Writable mode.
     */
    void transitionToWritableMode();

    /**
     * Change the state of bookie to ReadOnly mode.
     */
    void transitionToReadOnlyMode();

}

