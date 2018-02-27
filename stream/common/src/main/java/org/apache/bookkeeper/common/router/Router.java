/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.common.router;

import java.io.Serializable;

/**
 * Router to route the events to given ranges.
 */
public interface Router<K1, K2> extends Serializable {

    /**
     * Compute the routing key for a given key.
     *
     * <p>The routing key is used for routing events to {@code ranges}.
     *
     * @param key key of the events
     * @return the bytes representation of the provide event {@code key}.
     */
    K2 getRoutingKey(K1 key);

}
