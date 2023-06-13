package org.apache.bookkeeper.common.component;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

//This is the first step of the top-down strategy of the integration test.
//Therefore, I have tested LifecycleComponentStack only using mocks.

public class LifecycleComponentStackTest {

    private LifecycleComponentStack lifecycleComponentStack;    // tested object
    private LifecycleListener listener;
    private final String name = "lifecycle";
    private ComponentInfoPublisher publisher;

    @Before
    public void setUp() {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();

        LifecycleComponent component1 = mock(LifecycleComponent.class);
        when(component1.lifecycleState()).thenReturn(Lifecycle.State.INITIALIZED);
        LifecycleComponent component2 = mock(LifecycleComponent.class);
        when(component2.lifecycleState()).thenReturn(Lifecycle.State.INITIALIZED);

        publisher = mock(ComponentInfoPublisher.class);

        builder.withName(name).addComponent(component1).withComponentInfoPublisher(publisher).addComponent(component2);

        lifecycleComponentStack = builder.build();

        listener = mock(LifecycleListener.class);

        lifecycleComponentStack.addLifecycleListener(listener);
    }

    //Test per la costruzione di un LifecycleComponentStack vuoto
    @Test
    public void testBuild1() {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();

        try {
            builder.withName(name).build();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(true);
            return;
        }
        Assert.fail();
    }

    @Test
    public void testBuild2() {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();
        try {
            builder.withName(null).build();
        } catch (NullPointerException e) {
            Assert.assertTrue(true);
            return;
        }
        Assert.fail();
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
//            verify(lifecycleComponentStack.getComponent(i), times(1)).publishInfo(any());
//            verify(publisher, times(1)).startupFinished();
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
    public void testGetName() {
        Assert.assertEquals(name, lifecycleComponentStack.getName());
    }

    @Test
    public void testGetNumComponents() {
        Assert.assertEquals(lifecycleComponentStack.getNumComponents(), 2);
    }

    @Test
    public void testLifecycleState() {
        Assert.assertEquals(Lifecycle.State.INITIALIZED, lifecycleComponentStack.lifecycleState());
    }

    @Test
    public void testAddLifecycleListener() {
        LifecycleListener newListener = mock(LifecycleListener.class);

        lifecycleComponentStack.addLifecycleListener(newListener);

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            //Two invocations are expected because there is a first invocation in the @Before method
            verify(lifecycleComponentStack.getComponent(i), times(2)).addLifecycleListener(any());
        }
    }

    @Test
    public void testSetExceptionHandler() {
        Thread.UncaughtExceptionHandler handler = mock(Thread.UncaughtExceptionHandler.class);

        lifecycleComponentStack.setExceptionHandler(handler);

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            verify(lifecycleComponentStack.getComponent(i), times(1)).setExceptionHandler(any());
        }
    }

    @Test
    public void testRemoveLifecycleListener() {
        lifecycleComponentStack.removeLifecycleListener(listener);

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            verify(lifecycleComponentStack.getComponent(i), times(1)).removeLifecycleListener(any());
        }
    }
}