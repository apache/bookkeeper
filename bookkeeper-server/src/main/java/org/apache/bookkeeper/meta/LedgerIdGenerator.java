/**
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

import java.io.Closeable;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;

/**
 * The interface for global unique ledger ID generation
 */
public interface LedgerIdGenerator extends Closeable {

    /**
     * generate a global unique ledger id
     *
     * @param cb
     *            Callback when a new ledger id is generated, return code:<ul>
     *            <li>{@link BKException.Code.OK} if success</li>
     *            <li>{@link BKException.Code.ZKException} when can't generate new ledger id</li>
     *            </ul>
     */
    public void generateLedgerId(GenericCallback<Long> cb);

}
