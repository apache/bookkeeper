import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityCheck;
import org.apache.bookkeeper.bookie.datainteg.DataIntegrityService;
import org.apache.bookkeeper.common.component.ComponentInfoPublisher;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.common.component.LifecycleComponentStack;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
import org.apache.bookkeeper.server.service.StatsProviderService;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.Mockito.mock;

//This is the second step of the top-down strategy of the integration test.
//Here I have used the implementations of the class LifecycleComponent, instead of using mocks.

@RunWith(value= Parameterized.class)
public class LifecycleComponentStackTest {

    private LifecycleComponentStack lifecycleComponentStack;    // tested object
    private List<LifecycleComponent> components;
    private boolean isExpectedException;
    int statsProviderComponents;
    int dataIntegrityComponents;
    int nullComponents;
    int closedComponents;
    int duplicatedComponents;
    String name;



    private static List<LifecycleComponent> createComponentList(int statsProviderComponents, int dataIntegrityComponents, int nullComponents, int closedComponents, int duplicatedComponents) throws Exception {
        List<LifecycleComponent> list = new ArrayList<>();

        StatsLogger statsLogger = NullStatsLogger.INSTANCE; //It is a dummy stats logger originally present used for testing

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());

        //Adding StatsProviderService components
        for(int i = 0; i < statsProviderComponents; i++){
            StatsProviderService newComponent = new StatsProviderService(conf);

            list.add(newComponent);
        }

        //Adding DataIntegrityService components
        for(int i = 0; i < dataIntegrityComponents; i++){
            DataIntegrityCheck check = mock(DataIntegrityCheck.class);  //The implementation require to instantiate a bookie
            DataIntegrityService newComponent = new DataIntegrityService(conf, statsLogger, check);

            list.add(newComponent);
        }

        //Adding null components
        for(int i = 0; i < nullComponents; i++){
            list.add(null);
        }

        //Adding duplicated components
        int count = 0;
        if(duplicatedComponents > 0) {
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != null) {
                    list.add(list.get(i));
                    count++;
                    if (count >= duplicatedComponents) break;
                }

            }
        }

        //Adding closed components
        count = 0;
        if(closedComponents > 0) {
            for (LifecycleComponent component : list) {
                if (component != null) {
                    component.close();
                    count++;
                    if (count == closedComponents) break;
                }
            }
        }

        return list;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        //We use as parameters the different implementations of the class LifecycleComponent
        /*List of the implementations considered:
        *   - StatsProviderService
        *   - AutoRecoveryService
        *   - HttpService
        *   - AutoCloseableLifecycleComponent
        *   - RxSchedulerLifecycleComponent
        *   - BookieService
        *   - ScrubberService
        *   - DataIntegrityService
        *   - ServerLifecycleComponent*/

        return Arrays.asList(new Object[][]{
                //statsProviderComponents   dataIntegrityComponents      nullComponents          closedComponents       duplicatedComponents           isExpectedException
                //A component list with once of all the implementation choose
                {       1,                              1,                      0,                      0,                      0,                          false},
                //A component list with one of the LifecycleComponent inserted twice
                {       1,                              2,                      0,                      0,                      2,                          false},
                //A component list without any component
                {       0,                              0,                      0,                      0,                      0,                          true},
                //A component list with null components
                {       1,                              2,                      2,                      0,                      0,                          true},
                //A component list with closed components
                {       2,                              1,                      0,                      1,                      0,                          true},
        });
    }

    public LifecycleComponentStackTest(int statsProviderComponents, int dataIntegrityComponents, int nullComponents, int closedComponents, int duplicatedComponents, boolean isExpectedException) {
        this.isExpectedException = isExpectedException;
        this.statsProviderComponents = statsProviderComponents;
        this.dataIntegrityComponents = dataIntegrityComponents;
        this.nullComponents = nullComponents;
        this.closedComponents = closedComponents;
        this.duplicatedComponents = duplicatedComponents;
    }

    private LifecycleComponentStack getLifecycleComponentStack(List<LifecycleComponent> components) {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();

        name = "myStack";

        builder.withName(name);

        for(LifecycleComponent component: components) {
            builder.addComponent(component);
        }

        return builder.build();
    }

    private void setUp() throws Exception {
        components = createComponentList(statsProviderComponents, dataIntegrityComponents, nullComponents, closedComponents, duplicatedComponents);
        lifecycleComponentStack = getLifecycleComponentStack(components);
    }

    @Test
    public void testStart() {
        try {
            setUp();

            lifecycleComponentStack.start();

            for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
                Assert.assertEquals(Lifecycle.State.STARTED, lifecycleComponentStack.getComponent(i).lifecycleState());
            }
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }

    @Test
    public void testStop() {
        try {
            setUp();

            lifecycleComponentStack.start();    //If the components are not in the STARTED state, they cannot be stopped.

            lifecycleComponentStack.stop();

            for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
                Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
            }
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }

    @Test
    public void testGetName() {
        try {
            setUp();

            Assert.assertEquals(lifecycleComponentStack.getName(), name);
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException && (hasNullComponents() || isAnEmptyList())) {
            Assert.fail();
        }
    }

    private boolean hasNullComponents() {
        return nullComponents != 0;
    }

    private boolean isAnEmptyList() {
        return statsProviderComponents == 0 && dataIntegrityComponents == 0 && nullComponents == 0;
    }

    @Test
    public void testClose() {
        try {
            setUp();

            lifecycleComponentStack.close();

            for (int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
                Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
            }
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException && hasNullComponents()) {
            Assert.fail();
        }
    }


    /** Each lifecycle state cannot change in every state. The possible state changes are:
     * <ul>
     * <li>INITIALIZED -&gt; STARTED, STOPPED, CLOSED</li>
     * <li>STARTED     -&gt; STOPPED</li>
     * <li>STOPPED     -&gt; STARTED, CLOSED</li>
     * <li>CLOSED      -&gt; </li>
     * </ul>
    * While, the impossible states changes are:
    * <li>For each state is impossible return in the INITIALIZED state</li>
     * <li>CLOSED      -&gt; STARTED, STOPPED </li>
     * <li>STARTED     -&gt; CLOSED (it must pass through the STOPPED state before)</li>
    * */
    @Test
    public void testLifecycle1() {
        //This test tests the normal case when the lifecycle is respected
        try {
            setUp();

            lifecycleComponentStack.start();

            lifecycleComponentStack.stop();

            lifecycleComponentStack.close();
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }
    @Test
    public void testLifecycle2() {
        //This test try to restart the components
        try {
            setUp();

            lifecycleComponentStack.start();

            lifecycleComponentStack.stop();

            lifecycleComponentStack.start();

            lifecycleComponentStack.stop();

            lifecycleComponentStack.close();
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }

    @Test
    public void testLifecycle3() {
        //This test try to restart closed components
        try {
            setUp();

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
            if(!condition) Assert.fail();
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }

    @Test
    public void testLifecycle4() {
        //This test try to close started components
        try {
            setUp();

            lifecycleComponentStack.start();

            //Started components cannot be closed (must be stopped before), so an exception should be throws.
            //But this behaviour is managed by AbstractLifecycleComponent.

            lifecycleComponentStack.close();

        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }

    @Test
    public void testLifecycle5() {
        //This test try to stop closed components
        try {
            setUp();

            lifecycleComponentStack.start();

            lifecycleComponentStack.close();

            //Started components cannot be closed (must be stopped before), so an exception is expected.
            boolean condition = false;
            try {
                lifecycleComponentStack.stop();
            } catch (IllegalStateException e) {
                condition = true;
            }
            if(!condition) Assert.fail();
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }

    @Test
    public void testLifecycle6() {
        //This test try to stop initialized components
        try {
            setUp();

            lifecycleComponentStack.stop();
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException) {
            Assert.fail();
        }
    }

    @Test
    public void testLifecycle7() {
        //This test try to start closed components
        try {
            setUp();

            lifecycleComponentStack.close();
            boolean condition = false;
            try {
                lifecycleComponentStack.start();
            } catch (IllegalStateException e) {
                condition = true;
            }
            if(!condition) Assert.fail();
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException && hasNullComponents()) {
            Assert.fail();
        }
    }

    @Test
    public void testLifecycle8() {
        //This test cover the remained cases
        try {
            setUp();

            //Starting started components
            lifecycleComponentStack.start();

            lifecycleComponentStack.start();

            //Stopping stopped components
            lifecycleComponentStack.stop();

            lifecycleComponentStack.stop();

            //Closing closed components
            lifecycleComponentStack.close();

            lifecycleComponentStack.close();
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        if(isExpectedException && hasNullComponents()) {
            Assert.fail();
        }
    }

    @Test
    public void testPublishInfo() {
        try {
            setUp();

            ComponentInfoPublisher componentInfoPublisher = new ComponentInfoPublisher();

            lifecycleComponentStack.publishInfo(componentInfoPublisher);
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
            Assert.fail();
        }

        //In some implementation this method does not do anything, so, although there are closed components, exceptions are not expected.

    }
}