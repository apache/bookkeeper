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

package org.apache.bookkeeper.statelib.api;

import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.bookkeeper.statelib.api.exceptions.StateStoreException;

/**
 * A store that holds the states for stateful applications. Those stateful applications
 * can be databases, metadata services or stream processors.
 *
 * <p>This interface only specifies the basic functionality for saving and restoring state,
 * and its lifecycle management. Other capabilities are deferred to stateful applications.
 */
@Public
@Evolving
public interface StateStore extends AutoCloseable {

    /**
     * Returns the name of the state store.
     *
     * @return the name of the state store.
     */
    String name();

    /**
     * Initialize the state store.
     */
    void init(StateStoreSpec spec) throws StateStoreException;

    /**
     * Provides a mechanism to checkpoint the local state store to a remote checkpoint store specified
     * at {@link StateStoreSpec#getCheckpointStore()}.
     */
    default void checkpoint() {
    }

    /**
     * Get the last revision of the updates applied to this state store.
     *
     * @return last revision of the updates applied to this state store.
     */
    long getLastRevision();

    /**
     * Flush the state store to persistent medium.
     *
     * <p>Upon the call is returned, the state store should be durably persisted.</p>
     */
    void flush() throws StateStoreException;

    /**
     * Close the state store.
     */
    @Override
    void close();

}
