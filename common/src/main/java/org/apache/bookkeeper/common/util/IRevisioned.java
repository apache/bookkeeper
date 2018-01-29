/**
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
package org.apache.bookkeeper.common.util;

/**
 * Revisioned object.
 *
 * <p>Operations like updating offsets, updating metadata that modifies states
 * will be assigned with increasing revision number. The revision number can be used for
 * cas (compare-and-set) operation to guarantee consistent operations.
 */
public interface IRevisioned {

    long ANY_REVISION = -1L;

    /**
     * Return the revision associated with the value.
     *
     * @return the revision associated with the value.
     */
    long getRevision();

}
