package integration_testing;

import org.apache.bookkeeper.common.component.LifecycleListener;

public class LifecycleListenerTestImpl implements LifecycleListener {

    private final StringBuilder log;

    public LifecycleListenerTestImpl() {
        log = new StringBuilder();
    }

    public String getLog() {
        return log.toString();
    }

    @Override
    public void beforeStart() {
        log.append("Before Start\n");
    }

    @Override
    public void afterStart() {
        log.append("After Start\n");
    }

    @Override
    public void beforeStop() {
        log.append("Before Stop\n");
    }

    @Override
    public void afterStop() {
        log.append("After Stop\n");
    }

    @Override
    public void beforeClose() {
        log.append("Before Close\n");
    }

    @Override
    public void afterClose() {
        log.append("After Close\n");
    }
}
