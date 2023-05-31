package org.apache.bookkeeper.common.component;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

public class LifecycleComponentStackTest {

    private LifecycleComponentStack lifecycleComponentStack;
    LifecycleComponent component1;
    LifecycleComponent component2;
    Lifecycle.State state;

    @Before
    public void setUp() {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();

        component1 = mock(LifecycleComponent.class);
        when(component1.lifecycleState()).thenReturn(Lifecycle.State.INITIALIZED);
        component2 = mock(LifecycleComponent.class);
        when(component2.lifecycleState()).thenReturn(Lifecycle.State.STARTED);

        builder.withName("lifecycle").addComponent(component1).addComponent(component2);

        lifecycleComponentStack = builder.build();
    }

    @Test
    public void testStart() {
        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            int finalI = i;
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) {
                    when(lifecycleComponentStack.getComponent(finalI).lifecycleState()).thenReturn(Lifecycle.State.STARTED);
                    return null;
                }
            }).when(lifecycleComponentStack.getComponent(i)).start();
        }

        lifecycleComponentStack.start();

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            verify(lifecycleComponentStack.getComponent(i), times(1)).start();
            Assert.assertEquals(Lifecycle.State.STARTED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }
    }

    @Test
    public void testStop() {
        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            int finalI = i;
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) {
                    when(lifecycleComponentStack.getComponent(finalI).lifecycleState()).thenReturn(Lifecycle.State.STOPPED);
                    return null;
                }
            }).when(lifecycleComponentStack.getComponent(i)).stop();
        }

        lifecycleComponentStack.stop();

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            verify(lifecycleComponentStack.getComponent(i), times(1)).stop();
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }
    }

    @Test
    public void testClose() {
        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            int finalI = i;
            doAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocationOnMock) {
                    when(lifecycleComponentStack.getComponent(finalI).lifecycleState()).thenReturn(Lifecycle.State.CLOSED);
                    return null;
                }
            }).when(lifecycleComponentStack.getComponent(i)).close();
        }

        lifecycleComponentStack.close();

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            verify(lifecycleComponentStack.getComponent(i), times(1)).close();
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }
    }

    @Test
    public void testPublishInfo() {
        ComponentInfoPublisher publisher = mock(ComponentInfoPublisher.class);

        lifecycleComponentStack.publishInfo(publisher);

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            verify(lifecycleComponentStack.getComponent(i), times(1)).publishInfo(any());
        }
    }

    @Test
    public void testGetNumComponents() {
        Assert.assertEquals(lifecycleComponentStack.getNumComponents(), 2);
    }

    @Test
    public void testLifecycleState() {
        Assert.assertEquals(Lifecycle.State.INITIALIZED, lifecycleComponentStack.lifecycleState());
    }
}