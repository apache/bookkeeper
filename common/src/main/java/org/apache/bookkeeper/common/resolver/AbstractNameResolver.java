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

package org.apache.bookkeeper.common.resolver;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.concurrent.GuardedBy;
import org.apache.bookkeeper.common.util.SharedResourceManager;
import org.apache.bookkeeper.common.util.SharedResourceManager.Resource;

/**
 * The abstract bookkeeper name resolver.
 *
 * <p>The bookkeeper name resolvers should extend this one instead of {@link io.grpc.NameResolver}.
 */
public abstract class AbstractNameResolver extends NameResolver {

    private final String authority;
    private final Resource<ExecutorService> executorService;
    private final Runnable resolutionTask;

    @GuardedBy("this")
    private boolean shutdown;
    @GuardedBy("this")
    private boolean resolving;
    @GuardedBy("this")
    private Listener listener;
    @GuardedBy("this")
    private ExecutorService executor;

    private final class ResolverTask implements Runnable {

        @Override
        public void run() {
            Listener savedListener;
            synchronized (AbstractNameResolver.this) {
                if (shutdown) {
                    return;
                }
                resolving = true;
                savedListener = listener;
            }

            try {
                List<EquivalentAddressGroup> servers = getServers();
                savedListener.onAddresses(servers, Attributes.EMPTY);
            } finally {
                synchronized (AbstractNameResolver.this) {
                    resolving = false;
                }
            }
        }
    }

    protected AbstractNameResolver(String name,
                                   Resource<ExecutorService> executorResource) {
        URI nameURI = URI.create("//" + name);
        this.executorService = executorResource;
        this.authority = checkNotNull(nameURI.getAuthority(), "Name URI (%s) doesn't have an authority", nameURI);
        this.resolutionTask = new ResolverTask();
    }


    @Override
    public String getServiceAuthority() {
        return authority;
    }

    @Override
    public synchronized void start(Listener listener) {
        checkState(null == this.listener, "Resolver already started");
        this.executor = SharedResourceManager.shared().get(executorService);
        this.listener = checkNotNull(listener, "Listener is null");
        resolve();
    }

    @Override
    public synchronized void refresh() {
        checkState(null != listener, "Resolver has not started yet.");
        resolve();
    }

    @GuardedBy("this")
    private void resolve() {
        if (resolving || shutdown) {
            return;
        }
        executor.execute(resolutionTask);
    }

    protected abstract List<EquivalentAddressGroup> getServers();

    @Override
    public synchronized void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        if (null != executor) {
            SharedResourceManager.shared().release(executorService, executor);
        }
    }
}
