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

package org.apache.distributedlog.statestore.api;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.distributedlog.common.coder.Coder;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Specification for a state store.
 */
@FreeBuilder
public interface StateStoreSpec {

    /**
     * Returns the name of the state store.
     *
     * @return the name of the state store.
     */
    String name();

    /**
     * Returns the key coder.
     *
     * @return the key coder used by state store.
     */
    Coder<?> keyCoder();

    /**
     * Returns the value coder.
     *
     * @return the value coder used by state store.
     */
    Coder<?> valCoder();

    /**
     * Returns the local directory that is used for storing state locally.
     *
     * @return the local directory that is used for storing state locally.
     */
    Optional<File> localStateStoreDir();

    /**
     * Returns the distributedlog stream name that is used for storing updates durably.
     *
     * @return the stream name.
     */
    Optional<String> stream();

    /**
     * Returns the i/o scheduler used by the state store.
     *
     * @return the i/o scheduler
     */
    Optional<ScheduledExecutorService> ioScheduler();

    /**
     * Returns all the store specific config properties as key/value pairs.
     *
     * @return all the store specific config properties.
     */
    Map<String, Object> configs();

    /**
     * Builder to build a store specification.
     */
    class Builder extends StateStoreSpec_Builder {}

    /**
     * Construct a specification builder.
     *
     * @return builder to build a state store specification.
     */
    static Builder newBuilder() {
        return new Builder();
    }

}
