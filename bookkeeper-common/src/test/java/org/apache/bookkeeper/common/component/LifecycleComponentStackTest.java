package org.apache.bookkeeper.common.component;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.mockito.Mockito.*;

//This is the first step of the top-down strategy of the integration test.
//Therefore, I have tested LifecycleComponentStack only using mocks.

//@RunWith(value= Parameterized.class)
public class LifecycleComponentStackTest {

    private LifecycleComponentStack lifecycleComponentStack;    // tested object
    LifecycleComponent component1;
    LifecycleComponent component2;

    /*@Parameterized.Parameters
    public static Collection<Object[]> getParameters(){

        return Arrays.asList(new Object[][]{
                {2, 1, 1}, // expected, valueOne, valueTwo
                {3, 2, 1}, // expected, valueOne, valueTwo
                {4, 3, 1}, // expected, valueOne, valueTwo
        });
    }

    public LifecycleComponentStackTest(LifecycleComponentStack componentStack) {

    }*/

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