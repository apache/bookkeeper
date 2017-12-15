package org.apache.bookkeeper.util;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;

/**
 * A utility class for testing logger output.
 */
public class LoggerOutput implements TestRule {

    private Appender logAppender;
    private ArgumentCaptor<LoggingEvent> logEventCaptor;
    private List<Consumer<List<LoggingEvent>>> logEventExpectations = new ArrayList<>();

    public void expect(Consumer<List<LoggingEvent>> expectation) {
        if (logEventCaptor == null) {
            logEventCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
        }
        logEventExpectations.add(expectation);
    }

    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                logAppender = mock(Appender.class);
                Logger rootLogger = LogManager.getRootLogger();
                rootLogger.addAppender(logAppender);
                try {
                    base.evaluate();
                    if (!logEventExpectations.isEmpty()) {
                        verify(logAppender, atLeastOnce()).doAppend(logEventCaptor.capture());
                        List<LoggingEvent> logEvents = logEventCaptor.getAllValues();
                        for (Consumer<List<LoggingEvent>> expectation : logEventExpectations) {
                            expectation.accept(logEvents);
                        }
                    }
                } finally {
                    rootLogger.removeAppender(logAppender);
                    logEventExpectations.clear();
                    logEventCaptor = null;
                }
            }
        };
    }
}
