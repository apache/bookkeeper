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
package org.apache.distributedlog.util;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.distributedlog.io.AsyncCloseable;
import org.apache.distributedlog.io.AsyncDeleteable;
import org.apache.distributedlog.util.Transaction.OpListener;

/**
 * A common interface to allocate <i>R</i> under transaction <i>T</i>.
 *
 * <h3>Usage Example</h3>
 *
 *<p> Here is an example on demonstrating how `Allocator` works.</p>
 *
 * <pre> {@code
 * Allocator&lt;R, T&gt; allocator = ...;
 *
 * // issue an allocate request
 * try {
 *   allocator.allocate();
 * } catch (IOException ioe) {
 *   // handle the exception
 *   ...
 *   return;
 * }
 *
 * // Start a transaction
 * final Transaction<T> txn = ...;
 *
 * // Try obtain object R
 * CompletableFuture&lt;R&gt; tryObtainFuture = allocator.tryObtain(txn, new OpListener&lt;R&gt;() {
 *     public void onCommit(R resource) {
 *         // the obtain succeed, process with the resource
 *     }
 *     public void onAbort() {
 *         // the obtain failed.
 *     }
 * }).addFutureEventListener(new FutureEventListener() {
 *     public void onSuccess(R resource) {
 *         // the try obtain succeed. but the obtain has not been confirmed or aborted.
 *         // execute the transaction to confirm if it could complete obtain
 *         txn.execute();
 *     }
 *     public void onFailure(Throwable t) {
 *         // handle the failure of try obtain
 *     }
 * });
 *
 * }</pre>
 */
public interface Allocator<R, T> extends AsyncCloseable, AsyncDeleteable {

    /**
     * Issue allocation request to allocate <i>R</i>.
     * The implementation should be non-blocking call.
     *
     * @throws IOException
     *          if fail to request allocating a <i>R</i>.
     */
    void allocate() throws IOException;

    /**
     * Try obtaining an <i>R</i> in a given transaction <i>T</i>. The object obtained is tentative.
     * Whether the object is obtained or aborted is determined by the result of the execution. You could
     * register a listener under this `tryObtain` operation to know whether the object is obtained or
     * aborted.
     *
     * <p>It is a typical two-phases operation on obtaining a resource from allocator.
     * The future returned by this method acts as a `prepare` operation, the resource is tentative obtained
     * from the allocator. The execution of the txn acts as a `commit` operation, the resource is confirmed
     * to be obtained by this transaction. <code>listener</code> is for the whole completion of the obtain.
     *
     *  <p><code>listener</code> is only triggered after `prepare` succeed. if `prepare` failed, no actions will
     * happen to the listener.
     *
     * @param txn
     *          transaction.
     * @return future result returning <i>R</i> that would be obtained under transaction <code>txn</code>.
     */
    CompletableFuture<R> tryObtain(Transaction<T> txn, OpListener<R> listener);

}
