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

import static org.mockito.Mockito.*;

//This is the second step of the top-down strategy of the integration test.
//Here I have used the implementations of the class LifecycleComponent, instead of using mocks.

@RunWith(value= Parameterized.class)
public class LifecycleComponentStackTest {

    private LifecycleComponentStack lifecycleComponentStack;    // tested object
    private List<LifecycleComponent> components;
    private boolean isExpectedException;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() throws Exception {
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

        StatsLogger statsLogger = NullStatsLogger.INSTANCE; //It is a dummy stats logger originally present used for testing

        BookieConfiguration conf = new BookieConfiguration(new ServerConfiguration());

        StatsProviderService statsProviderService = new StatsProviderService(conf);

        DataIntegrityCheck check = mock(DataIntegrityCheck.class);  //The implementation require to instantiate a bookie
        DataIntegrityService dataIntegrityService = new DataIntegrityService(conf, statsLogger, check);

        //A component list with once of all the implementation choose
        List<LifecycleComponent> normalList = Arrays.asList(statsProviderService, dataIntegrityService);

        //A component list with one of the LifecycleComponent inserted twice
        List<LifecycleComponent> listWithDuplicatedValues = Arrays.asList(statsProviderService, statsProviderService, dataIntegrityService);

        //A component list without any component
        List<LifecycleComponent> emptylist = new ArrayList<>();

        //A component list with null components
        List<LifecycleComponent> listWithNullValues = Arrays.asList(statsProviderService, null, null, dataIntegrityService);


        return Arrays.asList(new Object[][]{
                //components                            isExpectedException
                {normalList,                                false},
                {listWithDuplicatedValues,                  false},
                {emptylist,                                 true},
                {listWithNullValues,                        true}
        });
    }

    public LifecycleComponentStackTest(/*String stackName,*/ List<LifecycleComponent> components, boolean isExpectedException) {
        this.isExpectedException = isExpectedException;
        this.components = components;
    }

    private LifecycleComponentStack getLifecycleComponentStack(List<LifecycleComponent> components) {
        LifecycleComponentStack.Builder builder = LifecycleComponentStack.newBuilder();

        builder.withName("myStack");

        for(LifecycleComponent component: components) {
            builder.addComponent(component);
        }

        return builder.build();
    }

    @Test
    public void testStart() {
        try {
            lifecycleComponentStack = getLifecycleComponentStack(components);
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
        }


        lifecycleComponentStack.start();

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STARTED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }
    }

    @Test
    public void testStop() {
        try {
            lifecycleComponentStack = getLifecycleComponentStack(components);
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
        }

        lifecycleComponentStack.start();    //If the components are not in the STARTED state, they cannot be stopped.

        lifecycleComponentStack.stop();

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.STOPPED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }
    }

    /*@Test
    public void testClose() {
        try {
            lifecycleComponentStack = getLifecycleComponentStack(components);
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
        }

        lifecycleComponentStack.close();

        for(int i = 0; i < lifecycleComponentStack.getNumComponents(); i++) {
            Assert.assertEquals(Lifecycle.State.CLOSED, lifecycleComponentStack.getComponent(i).lifecycleState());
        }
    }*/

    @Test
    public void testPublishInfo() {
        try {
            lifecycleComponentStack = getLifecycleComponentStack(components);
        } catch (Exception e) {
            if(isExpectedException) {
                Assert.assertTrue(true);
                return;
            }
        }

        ComponentInfoPublisher componentInfoPublisher = new ComponentInfoPublisher();

        lifecycleComponentStack.publishInfo(componentInfoPublisher);
    }
}