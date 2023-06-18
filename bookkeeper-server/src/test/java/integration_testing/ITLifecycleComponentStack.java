package integration_testing;

import org.apache.bookkeeper.bookie.datainteg.DataIntegrityCheck;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityService;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ITLifecycleComponentStack {
    LifecycleComponentStack lifecycleComponentStack;
    LifecycleListenerTestImpl listener;

    private LifecycleComponentStack getLifecycleComponentStack(List<LifecycleComponent> components) {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();

        String name = "myStack";

        builder.withName(name);

        for(LifecycleComponent component: components) {
            builder.addComponent(component);
        }

        return builder.build();
    }

    private DataIntegrityService getLimitedDataIntegrityService(BookieConfiguration conf, StatsLogger statsLogger, DataIntegrityCheck check) {
        return new DataIntegrityService(conf, statsLogger, check) {
            @Override
            public int interval() {
                return 500; //half second
            }
            @Override
            public TimeUnit intervalUnit() {
                return TimeUnit.MILLISECONDS;
            }
        };
    }

    private void validSetUp() throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; //It is a dummy stats logger originally present used for testing

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());

        DataIntegrityCheck check = mock(DataIntegrityCheck.class);  //The implementation require to instantiate a bookie
        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);

        LifecycleComponent component2 = new StatsProviderService(conf);

        List<LifecycleComponent> components = List.of(component1, component2);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    private void invalidSetUp(Class<? extends Throwable> t) throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; //It is a dummy stats logger originally present used for testing

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());

        DataIntegrityCheck check = mock(DataIntegrityCheck.class);
        CompletableFuture<Void> future = mock(CompletableFuture.class);
        when(check.needsFullCheck()).thenReturn(true);
        when(check.runFullCheck()).thenReturn(future);
        when(future.get()).thenThrow(t);


        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);

        LifecycleComponent component2 = new StatsProviderService(conf);

        List<LifecycleComponent> components = List.of(component1, component2);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    private void failSetUp() throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; //It is a dummy stats logger originally present used for testing

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());
        DataIntegrityCheck check = mock(DataIntegrityCheck.class);  //The implementation require to instantiate a bookie
        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);
        LifecycleComponent component2 = new StatsProviderService(conf);

        LifecycleComponent component3 = new DataIntegrityService(conf, statsLogger, check) {
            @Override
            public int interval() {
                return 500; //half second
            }
            @Override
            public TimeUnit intervalUnit() {
                return TimeUnit.MILLISECONDS;
            }
            @Override
            public void doStart() {
                throw new RuntimeException();
            }
        };

        List<LifecycleComponent> components = List.of(component1, component2, component3);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    private void failSetUp2() throws Exception {
        StatsLogger statsLogger = NullStatsLogger.INSTANCE; //It is a dummy stats logger originally present used for testing

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());
        DataIntegrityCheck check = mock(DataIntegrityCheck.class);  //The implementation require to instantiate a bookie
        LifecycleComponent component1 = getLimitedDataIntegrityService(conf, statsLogger, check);
        LifecycleComponent component2 = new StatsProviderService(conf);

        LifecycleComponent component3 = new DataIntegrityService(conf, statsLogger, check) {
            @Override
            public int interval() {
                return 500; //half second
            }
            @Override
            public TimeUnit intervalUnit() {
                return TimeUnit.MILLISECONDS;
            }
            @Override
            public void doStop() {
                close();
            }
        };

        List<LifecycleComponent> components = List.of(component1, component2, component3);

        lifecycleComponentStack = getLifecycleComponentStack(components);

        listener = new LifecycleListenerTestImpl();
        lifecycleComponentStack.addLifecycleListener(listener);
    }

    @Test
    public void testStartAndStop() throws Exception {

        validSetUp();

        lifecycleComponentStack.start();    //If the components are not in the STARTED state, they cannot be stopped.

        Thread.sleep(1000);

        lifecycleComponentStack.stop();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

    }

    //This test cover the case wherein the DataIntegrityService has a problem. The component shouldn't be interrupted.
    @Test
    public void testInvalidStartAndStop() throws Exception {

        invalidSetUp(InterruptedException.class);

        lifecycleComponentStack.start();    //If the components are not in the STARTED state, they cannot be stopped.

        Thread.sleep(1200);

        lifecycleComponentStack.stop();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

    }

    //This test cover the case wherein the DataIntegrityService has a problem. The component shouldn't be interrupted.
    @Test
    public void testInvalid2StartAndStop() throws Exception {

        invalidSetUp(ExecutionException.class);

        lifecycleComponentStack.start();    //If the components are not in the STARTED state, they cannot be stopped.

        Thread.sleep(1200);

        lifecycleComponentStack.stop();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

    }

    @Test
    public void testClose() throws Exception {

        validSetUp();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        Assert.assertEquals(
                "Before Close\nAfter Close\n" +
                "Before Close\nAfter Close\n", listener.getLog());
    }

    @Test
    public void testStart() throws Exception {

        validSetUp();

        lifecycleComponentStack.start();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STARTED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        Assert.assertEquals(
                "Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n", listener.getLog());
    }

    //This test simulate an exception in a component and how the stack react. An ExceptionHandler is also configured, so the test verify
    // also that the handler is right configured in the components.
    @Test
    public void testFail1() throws Exception {

        failSetUp();

        Semaphore semaphore = new Semaphore(1); //A semaphore is used to see if the handler has been called

        lifecycleComponentStack.setExceptionHandler((t, e) -> semaphore.release());

        semaphore.acquire();

        lifecycleComponentStack.start();

        semaphore.acquire();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Close\nAfter Close\n" +
                "Before Close\nAfter Close\n" +
                "Before Close\nAfter Close\n", listener.getLog());
    }

    //This test simulate an exception in a component and how the stack react. No ExceptionHandler are configured, so the test start() method must
    // throw an exception.
    @Test
    public void testFail2() throws Exception {

        failSetUp();
        try {
            lifecycleComponentStack.start();
        } catch (RuntimeException e) {
            //catching the exception
        }

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Start\n", listener.getLog());
    }

    @Test
    public void testLifecycle1() throws Exception {
        //This test tests the normal case when the lifecycle is respected

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Close\nAfter Close\n" +
                "Before Close\nAfter Close\n", listener.getLog());
    }

    @Test
    public void testLifecycle2() throws Exception {
        //This test try to restart the components

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Close\nAfter Close\n" +
                "Before Close\nAfter Close\n", listener.getLog());

    }

    @Test
    public void testLifecycle3() throws Exception {
        //This test try to restart closed components

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        lifecycleComponentStack.close();

        //Closed components cannot be restarted, so an exception is expected.
        boolean condition = false;
        try {
            lifecycleComponentStack.start();
        } catch (IllegalStateException e) {
            condition = true;
        }
        Assert.assertTrue(condition);

    }

    @Test
    public void testLifecycle4() throws Exception {
        //This test try to close started components

        validSetUp();

        lifecycleComponentStack.start();

        //Started components cannot be closed (must be stopped before), but this behaviour is managed.
        // So, before to goes in the CLOSED state, each component should go in the STOPPED state.

        lifecycleComponentStack.close();

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Close\nAfter Close\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Close\nAfter Close\n", listener.getLog());
    }

    @Test
    public void testLifecycle5() throws Exception {
        //This test try to stop closed components

        validSetUp();

        lifecycleComponentStack.start();

        lifecycleComponentStack.close();

        //Closed components cannot change their state, so an exception is expected.
        boolean condition = false;
        try {
            lifecycleComponentStack.stop();
        } catch (IllegalStateException e) {
            condition = true;
        }
        Assert.assertTrue(condition);

    }

    @Test@Ignore
    public void testLifecycle6() throws Exception {
        //This test try to stop initialized components, so the components don't do anything.

        validSetUp();

        lifecycleComponentStack.stop();

        //Verifying if all the listener operation (before and after each operation in each component) have been executed.
        Assert.assertEquals("Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n", listener.getLog());
    }

    @Test
    public void testLifecycle8() throws Exception {
        //This test cover the remained cases

        validSetUp();

        //Starting started components
        lifecycleComponentStack.start();

        lifecycleComponentStack.start();

        //Stopping stopped components
        lifecycleComponentStack.stop();

        lifecycleComponentStack.stop();

        //Closing closed components
        lifecycleComponentStack.close();

        lifecycleComponentStack.close();

        Assert.assertEquals("Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Close\nAfter Close\n" +
                "Before Close\nAfter Close\n", listener.getLog());

    }

    @Test
    public void testLifecycle9() throws Exception {
        //This test cover the case in which one of the component is going to have a bad behaviour. One of the component, instead to go
        // in the STOPPED state, goes in the CLOSED state. Hence, when the stack asks to restart, there will be a failure.

        failSetUp2();

        lifecycleComponentStack.start();

        lifecycleComponentStack.stop();

        try {
            lifecycleComponentStack.start();
        } catch (Exception e) {
            //Exception caught
        }

        Assert.assertEquals("Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n" +
                "Before Stop\n" +
                "Before Close\nAfter Close\n" +
                "After Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Stop\nAfter Stop\n" +
                "Before Start\nAfter Start\n" +
                "Before Start\nAfter Start\n", listener.getLog());

    }

}
