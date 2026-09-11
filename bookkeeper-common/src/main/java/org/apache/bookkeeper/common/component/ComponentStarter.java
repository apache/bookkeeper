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

import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Utils to start components.
 */
@CustomLog
public class ComponentStarter {

    static class ComponentShutdownHook implements Runnable {

        private final LifecycleComponent component;
        private final CompletableFuture<Void> future;

        ComponentShutdownHook(LifecycleComponent component,
                              CompletableFuture<Void> future) {
            this.component = component;
            this.future = future;
        }

        @Override
        public void run() {
            log.info().attr("component", component.getName()).log("Closing component in shutdown hook");
            try {
                component.close();
                log.info()
                        .attr("component", component.getName())
                        .log("Closed component in shutdown hook successfully. Exiting");
                FutureUtils.complete(future, null);
            } catch (Throwable e) {
                log.error()
                        .exception(e)
                        .attr("component", component.getName())
                        .log("Failed to close component in shutdown hook gracefully, Exiting anyway");
                future.completeExceptionally(e);
            }
        }

    }

    /**
     * Start a component and register a shutdown hook.
     *
     * @param component component to start.
     */
    public static CompletableFuture<Void> startComponent(LifecycleComponent component) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        final Thread shutdownHookThread = new Thread(
            new ComponentShutdownHook(component, future),
            "component-shutdown-thread"
        );

        // register a shutdown hook
        Runtime.getRuntime().addShutdownHook(shutdownHookThread);

        // register a component exception handler
        component.setExceptionHandler((t, e) -> {
            log.error()
                    .exception(e)
                    .attr("component", component.getName())
                    .attr("thread", t)
                    .log("Triggered exceptionHandler of Component because of Exception in Thread");
            System.err.println(e.getMessage());
            // start the shutdown hook when an uncaught exception happen in the lifecycle component.
            try {
                shutdownHookThread.start();
            } catch (IllegalThreadStateException ise) {
                // the shutdown hook thread is already running (e.g. triggered by a prior
                // exception or by the JVM shutdown sequence), so there is nothing else to do.
            }
        });

        component.publishInfo(new ComponentInfoPublisher());

        log.info().attr("component", component.getName()).log("Starting component");
        component.start();
        log.info().attr("component", component.getName()).log("Started component");
        return future;
    }

}
