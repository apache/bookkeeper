package org.apache.bookkeeper.common.component;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LifecycleTest {

    Lifecycle lifecycle;

    @Before
    public void beforeState() {
        lifecycle = new Lifecycle();
    }

    @Test
    public void state() {
        Assert.assertEquals(lifecycle.state(), Lifecycle.State.INITIALIZED);
    }

    @Test
    public void moveToStarted() {
        lifecycle.moveToStarted();
        if(lifecycle.state() != Lifecycle.State.STARTED) Assert.fail();

        try {
            lifecycle.moveToClosed();
            lifecycle.moveToStarted();
        } catch (IllegalStateException e) {
            Assert.assertTrue(true);
            return;
        }

        Assert.fail();
    }
}