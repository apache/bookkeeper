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

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mix of {@link AbstractComponent} and {@link LifecycleComponent}.
 */
@Slf4j
public abstract class AbstractLifecycleComponent<ConfT extends ComponentConfiguration>
    extends AbstractComponent<ConfT> implements LifecycleComponent {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractLifecycleComponent.class);

    protected final Lifecycle lifecycle = new Lifecycle();
    private final Set<LifecycleListener> listeners = new CopyOnWriteArraySet<>();
    protected final StatsLogger statsLogger;
    protected volatile UncaughtExceptionHandler uncaughtExceptionHandler;

    protected AbstractLifecycleComponent(String componentName,
                                         ConfT conf,
                                         StatsLogger statsLogger) {
        super(componentName, conf);
        this.statsLogger = statsLogger;
    }

    @Override
    public void setExceptionHandler(UncaughtExceptionHandler handler) {
        this.uncaughtExceptionHandler = handler;
    }

    protected StatsLogger getStatsLogger() {
        return statsLogger;
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
        try {
            doStart();
        } catch (Throwable exc) {
            LOG.error("Failed to start Component: {}", getName(), exc);
            if (uncaughtExceptionHandler != null) {
                LOG.error("Calling uncaughtExceptionHandler");
                uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), exc);
            } else {
                throw exc;
            }
        }
        lifecycle.moveToStarted();
        listeners.forEach(LifecycleListener::afterStart);
    }

    protected abstract void doStart();

    @Override
    public void stop() {
        if (!lifecycle.canMoveToStopped()) {
            return;
        }
        listeners.forEach(LifecycleListener::beforeStop);
        lifecycle.moveToStopped();
        doStop();
        listeners.forEach(LifecycleListener::afterStop);
    }

    protected abstract void doStop();

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
            doClose();
        } catch (IOException e) {
            log.warn("failed to close {}", getClass().getName(), e);
        }
        listeners.forEach(LifecycleListener::afterClose);
    }

    protected abstract void doClose() throws IOException;

}
