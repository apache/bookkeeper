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
package org.apache.hedwig.client.netty;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.util.Callback;

/**
 * Adapts from Callback&lt;T> to Callback&lt;Void>. (Ignores the &lt;T> parameter).
 */
public class VoidCallbackAdapter<T> implements Callback<T> {
    private final Callback<Void> delegate;

    public VoidCallbackAdapter(Callback<Void> delegate){
        this.delegate = delegate;
    }

    @Override
    public void operationFinished(Object ctx, T resultOfOperation) {
        delegate.operationFinished(ctx, null);
    }

    @Override
    public void operationFailed(Object ctx, PubSubException exception) {
        delegate.operationFailed(ctx, exception);
    }
}
