/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bookkeeper.common.component;

import io.reactivex.rxjava3.core.Scheduler;
import java.util.concurrent.ExecutorService;
import org.apache.bookkeeper.common.conf.ComponentConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * The scheduler for rxjava based jobs, such as data integrity checking.
 */
public class RxSchedulerLifecycleComponent extends AbstractLifecycleComponent<ComponentConfiguration> {
    private final Scheduler scheduler;
    private final ExecutorService rxExecutor;

    public RxSchedulerLifecycleComponent(String componentName,
                                         ComponentConfiguration conf,
                                         StatsLogger stats,
                                         Scheduler scheduler,
                                         ExecutorService rxExecutor) {
        super(componentName, conf, stats);
        this.scheduler = scheduler;
        this.rxExecutor = rxExecutor;
    }

    @Override
    protected void doStart() {
        scheduler.start();
    }

    @Override
    protected void doStop() {
        scheduler.shutdown();
        rxExecutor.shutdown();
    }

    @Override
    public void doClose() {
        scheduler.shutdown();
        rxExecutor.shutdown();
    }
}
