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

import static org.junit.Assert.assertEquals;
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
import org.junit.Test;

/**
 * Unit test for {@link LifecycleComponentStack}.
 */
public class TestLifecycleComponentStack {

  @Test(expected = NullPointerException.class)
  public void testBuilderWithNullName() {
    LifecycleComponentStack.newBuilder().withName(null).build();
  }

  @Test(expected = NullPointerException.class)
  public void testBuilderWithNullComponent() {
    LifecycleComponentStack.newBuilder().addComponent(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderWithEmptyComponentList() {
    LifecycleComponentStack.newBuilder().withName("empty-list").build();
  }

  @Test
  public void testGetName() {
    String name = "test-get-name";
    LifecycleComponentStack stack = LifecycleComponentStack.newBuilder()
      .withName(name)
      .addComponent(mock(LifecycleComponent.class))
      .build();
    assertEquals(name, stack.getName());
  }

  @Test
  public void testLifecycleState() {
    LifecycleComponent component1 = mock(LifecycleComponent.class);
    when(component1.lifecycleState()).thenReturn(Lifecycle.State.INITIALIZED);
    LifecycleComponent component2 = mock(LifecycleComponent.class);
    when(component2.lifecycleState()).thenReturn(Lifecycle.State.STARTED);

    LifecycleComponentStack stack = LifecycleComponentStack.newBuilder()
      .withName("get-lifecycle-state")
      .addComponent(component1)
      .addComponent(component2)
      .build();

    assertEquals(Lifecycle.State.INITIALIZED, stack.lifecycleState());
  }

  @Test
  public void testAddRemoveLifecycleListener() {
    LifecycleComponent component1 = mock(LifecycleComponent.class);
    LifecycleComponent component2 = mock(LifecycleComponent.class);

    LifecycleComponentStack stack = LifecycleComponentStack.newBuilder()
      .withName("get-lifecycle-listener")
      .addComponent(component1)
      .addComponent(component2)
      .build();

    LifecycleListener listener = mock(LifecycleListener.class);
    stack.addLifecycleListener(listener);
    verify(component1).addLifecycleListener(listener);
    verify(component2).addLifecycleListener(listener);
    stack.removeLifecycleListener(listener);
    verify(component1).removeLifecycleListener(listener);
    verify(component2).removeLifecycleListener(listener);
  }

  @Test
  public void testStartStopClose() {
    LifecycleComponent component1 = mock(LifecycleComponent.class);
    LifecycleComponent component2 = mock(LifecycleComponent.class);

    LifecycleComponentStack stack = LifecycleComponentStack.newBuilder()
      .withName("get-lifecycle-listener")
      .addComponent(component1)
      .addComponent(component2)
      .build();

    stack.start();
    verify(component1).start();
    verify(component2).start();
    stack.stop();
    verify(component1).stop();
    verify(component2).stop();
    stack.close();
    verify(component1).close();
    verify(component2).close();
  }

  @Test
  public void testSetExceptionHandler() {
    LifecycleComponent component1 = mock(LifecycleComponent.class);
    LifecycleComponent component2 = mock(LifecycleComponent.class);

    LifecycleComponentStack stack = LifecycleComponentStack.newBuilder()
        .withName("set-exception-handler-stack")
        .addComponent(component1)
        .addComponent(component2)
        .build();

    UncaughtExceptionHandler handler = mock(UncaughtExceptionHandler.class);

    stack.setExceptionHandler(handler);
    verify(component1, times(1)).setExceptionHandler(eq(handler));
    verify(component2, times(1)).setExceptionHandler(eq(handler));
  }

  @Test
  public void testExceptionHandlerShutdownLifecycleComponentStack() throws Exception {
    LifecycleComponent component1 = mock(LifecycleComponent.class);
    LifecycleComponent component2 = mock(LifecycleComponent.class);
    AtomicReference<UncaughtExceptionHandler> handlerRef1 = new AtomicReference<>();
    doAnswer(invocationOnMock -> {
        handlerRef1.set(invocationOnMock.getArgument(0));
        return null;
    }).when(component1).setExceptionHandler(any(UncaughtExceptionHandler.class));

    LifecycleComponentStack stack = LifecycleComponentStack.newBuilder()
        .withName("exception-handler-shutdown-lifecycle-component-stack")
        .addComponent(component1)
        .addComponent(component2)
        .build();

    CompletableFuture<Void> startFuture = ComponentStarter.startComponent(stack);
    verify(component1, times(1)).start();
    verify(component1, times(1)).setExceptionHandler(eq(handlerRef1.get()));
    verify(component2, times(1)).start();
    verify(component2, times(1)).setExceptionHandler(eq(handlerRef1.get()));

    // if an exception is signaled through any component,
    // the startFuture will be completed and all the components will be shutdown
    handlerRef1.get().uncaughtException(
        Thread.currentThread(), new Exception("exception-handler-shutdown-lifecycle-component-stack"));

    startFuture.get();
    verify(component1, times(1)).close();
    verify(component2, times(1)).close();
  }

}
