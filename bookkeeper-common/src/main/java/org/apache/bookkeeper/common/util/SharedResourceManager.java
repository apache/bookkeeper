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

package org.apache.bookkeeper.common.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.IdentityHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A holder for shared resource singletons.
 *
 * <p>Components like clients and servers need certain resources, e.g. a scheduler,
 * to run. If the user has not provided such resources, these components will use
 * a default one, which is shared as a static resource. This class holds these default
 * resources and manages their lifecycles.
 *
 * <p>A resource is identified by the reference of a {@link Resource} object, which
 * is typically a singleton, provided to the get() and release() methods. Each resource
 * object (not its class) maps to an object cached in the holder.
 *
 * <p>Resources are ref-counted and shut down after a delay when the refcount reaches zero.
 */
public class SharedResourceManager {

    static final long DESTROY_DELAY_SECONDS = 1;

    private static final SharedResourceManager SHARED = create();

    public static SharedResourceManager shared() {
        return SHARED;
    }

    public static SharedResourceManager create() {
        return create(() -> Executors.newSingleThreadScheduledExecutor(
            ExecutorUtils.getThreadFactory("bookkeeper-shared-destroyer-%d", true)));
    }

    public static SharedResourceManager create(Supplier<ScheduledExecutorService> destroyerFactory) {
        return new SharedResourceManager(destroyerFactory);
    }

    /**
     * Defines a resource, and the way to create and destroy instances of it.
     *
     * @param <T> resource type.
     */
    public interface Resource<T> {
        /**
         * Create a new instance of the resource.
         *
         * @return a new instance of the resource.
         */
        T create();

        /**
         * Destroy the given instance.
         *
         * @param instance the instance to destroy.
         */
        void close(T instance);
    }

    private static class Instance implements ReferenceCounted {

        private final Object instance;
        private int refCount;
        ScheduledFuture<?> destroyTask;

        Instance(Object instance) {
            this.instance = instance;
        }

        @Override
        public void retain() {
            ++refCount;
        }

        @Override
        public void release() {
            --refCount;
        }

        void cancelDestroyTask() {
            if (null != destroyTask) {
                destroyTask.cancel(false);
                destroyTask = null;
            }
        }

    }

    private final IdentityHashMap<Resource<?>, Instance> instances =
        new IdentityHashMap<>();
    private final Supplier<ScheduledExecutorService> destroyerFactory;
    private ScheduledExecutorService destroyer;

    private SharedResourceManager(Supplier<ScheduledExecutorService> destroyerFactory) {
        this.destroyerFactory = destroyerFactory;
    }

    @SuppressWarnings("unchecked")
    public synchronized <T> T get(Resource<T> resource) {
        Instance instance = instances.get(resource);
        if (null == instance) {
            instance = new Instance(resource.create());
            instances.put(resource, instance);
        }
        instance.cancelDestroyTask();
        instance.retain();
        return (T) instance.instance;
    }

    public synchronized <T> void release(final Resource<T> resource,
                                         final T instance) {
        final Instance cached = instances.get(resource);
        checkArgument(null != cached, "No cached instance found for %s", resource);
        checkArgument(instance == cached.instance, "Release the wrong instance for %s", resource);
        checkState(cached.refCount > 0, "Refcount has already reached zero for %s", resource);
        cached.release();
        if (0 == cached.refCount) {
            checkState(null == cached.destroyTask, "Destroy task already scheduled for %s", resource);
            if (null == destroyer) {
                destroyer = destroyerFactory.get();
            }
            cached.destroyTask = destroyer.schedule(new LogExceptionRunnable(() -> {
                synchronized (SharedResourceManager.this) {
                    // Refcount may have gone up since the task was scheduled. Re-check it.
                    if (cached.refCount == 0) {
                        resource.close(instance);
                        instances.remove(resource);
                        if (instances.isEmpty()) {
                            destroyer.shutdown();
                            destroyer = null;
                        }
                    }
                }
            }), DESTROY_DELAY_SECONDS, TimeUnit.SECONDS);
        }
    }


}
