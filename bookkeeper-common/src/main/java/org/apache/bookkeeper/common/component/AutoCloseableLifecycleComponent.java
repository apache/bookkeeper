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

package org.apache.bookkeeper.common.component;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows for AutoClosable resources to be added to the component
 * lifecycle without having to implement ServerLifecycleComponent directly.
 */
public class AutoCloseableLifecycleComponent implements LifecycleComponent {
    private static final Logger LOG = LoggerFactory.getLogger(AutoCloseableLifecycleComponent.class);

    protected final Lifecycle lifecycle = new Lifecycle();
    private final Set<LifecycleListener> listeners = new CopyOnWriteArraySet<>();
    protected volatile UncaughtExceptionHandler uncaughtExceptionHandler;
    private final String componentName;
    private final AutoCloseable closeable;

    public AutoCloseableLifecycleComponent(String componentName, AutoCloseable closeable) {
        this.componentName = componentName;
        this.closeable = closeable;
    }

    @Override
    public String getName() {
        return this.componentName;
    }

    @Override
    public void setExceptionHandler(UncaughtExceptionHandler handler) {
        this.uncaughtExceptionHandler = handler;
    }

    @Override
    public Lifecycle.State lifecycleState() {
        return this.lifecycle.state();
    }

    @Override
    public void addLifecycleListener(LifecycleListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeLifecycleListener(LifecycleListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void start() {
        if (!lifecycle.canMoveToStarted()) {
            return;
        }
        listeners.forEach(LifecycleListener::beforeStart);
        lifecycle.moveToStarted();
        listeners.forEach(LifecycleListener::afterStart);
    }

    @Override
    public void stop() {
        if (!lifecycle.canMoveToStopped()) {
            return;
        }
        listeners.forEach(LifecycleListener::beforeStop);
        lifecycle.moveToStopped();
        listeners.forEach(LifecycleListener::afterStop);
    }

    @Override
    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.canMoveToClosed()) {
            return;
        }
        listeners.forEach(LifecycleListener::beforeClose);
        lifecycle.moveToClosed();
        try {
            closeable.close();
        } catch (Exception e) {
            LOG.warn("failed to close {}", componentName, e);
        }
        listeners.forEach(LifecycleListener::afterClose);
    }
}
