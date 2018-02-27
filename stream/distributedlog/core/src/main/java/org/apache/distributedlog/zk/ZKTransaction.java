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
package org.apache.distributedlog.zk;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.util.Transaction;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.OpResult;

/**
 * ZooKeeper Transaction.
 */
public class ZKTransaction implements Transaction<Object>, AsyncCallback.MultiCallback {

    private final ZooKeeperClient zkc;
    private final List<ZKOp> ops;
    private final List<org.apache.zookeeper.Op> zkOps;
    private final CompletableFuture<Void> result;
    private final AtomicBoolean done = new AtomicBoolean(false);

    public ZKTransaction(ZooKeeperClient zkc) {
        this.zkc = zkc;
        this.ops = Lists.newArrayList();
        this.zkOps = Lists.newArrayList();
        this.result = new CompletableFuture<Void>();
    }

    @Override
    public void addOp(Op<Object> operation) {
        if (done.get()) {
            throw new IllegalStateException("Add an operation to a finished transaction");
        }
        assert(operation instanceof ZKOp);
        ZKOp zkOp = (ZKOp) operation;
        this.ops.add(zkOp);
        this.zkOps.add(zkOp.getOp());
    }

    @Override
    public CompletableFuture<Void> execute() {
        if (!done.compareAndSet(false, true)) {
            return result;
        }
        try {
            zkc.get().multi(zkOps, this, result);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            result.completeExceptionally(Utils.zkException(e, ""));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.completeExceptionally(Utils.zkException(e, ""));
        }
        return result;
    }

    @Override
    public void abort(Throwable cause) {
        if (!done.compareAndSet(false, true)) {
            return;
        }
        for (int i = 0; i < ops.size(); i++) {
            ops.get(i).abortOpResult(cause, null);
        }
        FutureUtils.completeExceptionally(result, cause);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<OpResult> results) {
        if (KeeperException.Code.OK.intValue() == rc) { // transaction succeed
            for (int i = 0; i < ops.size(); i++) {
                ops.get(i).commitOpResult(results.get(i));
            }
            FutureUtils.complete(result, null);
        } else {
            KeeperException ke = KeeperException.create(KeeperException.Code.get(rc));
            for (int i = 0; i < ops.size(); i++) {
                ops.get(i).abortOpResult(ke, null != results ? results.get(i) : null);
            }
            FutureUtils.completeExceptionally(result, ke);
        }
    }
}
