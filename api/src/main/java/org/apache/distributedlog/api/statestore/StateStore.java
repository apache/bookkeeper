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

package org.apache.distributedlog.api.statestore;

import org.apache.bookkeeper.common.annotation.InterfaceAudience.Public;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Evolving;
import org.apache.distributedlog.api.statestore.exceptions.StateStoreException;

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
     * This is a method called before calling {@link #init(StateStoreSpec)}. The implementation
     * can use this method to prepare environment for initialization. For example, loading
     * checkpoints from a {@link org.apache.distributedlog.api.statestore.checkpoint.CheckpointStore}.
     *
     * @param spec state store spec.
     * @throws StateStoreException
     */
    default void prepareInit(StateStoreSpec spec) throws StateStoreException {
    }

    /**
     * Initialize the state store.
     */
    void init(StateStoreSpec spec) throws StateStoreException;

    /**
     * Flush the state store to persistent medium.
     *
     * <p>Upon the call is returned, the state store should be durably persisted.</p>
     */
    void flush() throws StateStoreException;

    /**
     * Close the state store.
     */
    void close();

}
