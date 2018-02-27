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
package org.apache.bookkeeper.api.kv.result;

import org.apache.bookkeeper.api.kv.op.OpType;

/**
 * The base result.
 */
public interface Result<K, V> extends AutoCloseable {

    /**
     * Returns the pkey of this op.
     *
     * <p>null if pkey is not defined.
     *
     * @return the pkey used in this deletion.
     */
    K pKey();

    OpType type();

    Code code();

    /**
     * Returns the revision of this op.
     *
     * @return the revision of this op.
     */
    long revision();

    @Override
    void close();

}
