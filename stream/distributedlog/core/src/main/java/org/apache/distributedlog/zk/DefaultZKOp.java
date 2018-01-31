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

import javax.annotation.Nullable;
import org.apache.distributedlog.util.Transaction.OpListener;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;


/**
 * Default zookeeper operation. No action on commiting or aborting.
 */
public class DefaultZKOp extends ZKOp {

    public static DefaultZKOp of(Op op, OpListener<Void> listener) {
        return new DefaultZKOp(op, listener);
    }

    private final OpListener<Void> listener;

    private DefaultZKOp(Op op, @Nullable OpListener<Void> opListener) {
        super(op);
        this.listener = opListener;
    }

    @Override
    protected void commitOpResult(OpResult opResult) {
        if (null != listener) {
            listener.onCommit(null);
        }
    }

    @Override
    protected void abortOpResult(Throwable t, OpResult opResult) {
        if (null != listener) {
            listener.onAbort(t);
        }
    }
}
