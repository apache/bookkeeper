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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.component.ComponentStarter.ComponentShutdownHook;
import org.junit.Test;

/**
 * Test Case of {@link ComponentStarter}.
 */
public class TestComponentStarter {

  @Test
  public void testStartComponent() {
    LifecycleComponent component = mock(LifecycleComponent.class);
    when(component.getName()).thenReturn("test-start-component");
    ComponentStarter.startComponent(component);
    verify(component).publishInfo(any(ComponentInfoPublisher.class));
    verify(component).start();
  }

  @Test
  public void testComponentShutdownHook() throws Exception {
    LifecycleComponent component = mock(LifecycleComponent.class);
    when(component.getName()).thenReturn("test-shutdown-hook");
    CompletableFuture<Void> future = new CompletableFuture<>();
    ComponentShutdownHook shutdownHook = new ComponentShutdownHook(component, future);
    shutdownHook.run();
    verify(component).close();
    future.get();
  }

  @Test
  public void testExceptionHandler() throws Exception {
    // prepare a mock lifecycle component
    LifecycleComponent component = mock(LifecycleComponent.class);
    when(component.getName()).thenReturn("test-exception-handler");
    AtomicReference<UncaughtExceptionHandler> exceptionHandlerRef = new AtomicReference<>();
    doAnswer(invocationOnMock -> {
        UncaughtExceptionHandler handler = invocationOnMock.getArgument(0);
        exceptionHandlerRef.set(handler);
        return null;
    }).when(component).setExceptionHandler(any(UncaughtExceptionHandler.class));

    // start the future
    CompletableFuture<Void> startFuture = ComponentStarter.startComponent(component);
    verify(component, times(1)).start();
    verify(component, times(1)).setExceptionHandler(eq(exceptionHandlerRef.get()));

    // if an exception is signaled through exception handler,
    // the startFuture will be completed and the component will be shutdown
    exceptionHandlerRef.get().uncaughtException(
        Thread.currentThread(), new Exception("test-exception-handler"));

    startFuture.get();
    verify(component, times(1)).close();
  }

}
