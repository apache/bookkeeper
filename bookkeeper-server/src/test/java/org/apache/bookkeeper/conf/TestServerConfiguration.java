package org.apache.bookkeeper.conf;

import static org.junit.Assert.assertTrue;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

/**
 * Unit test for {@link ServerConfiguration}.
 */
public class TestServerConfiguration {

    @Test
    public void testEphemeralPortsAllowed() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowEphemeralPorts(true);
        conf.setBookiePort(0);

        conf.validate();
        assertTrue(true);
    }

    @Test(expected = ConfigurationException.class)
    public void testEphemeralPortsDisallowed() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowEphemeralPorts(false);
        conf.setBookiePort(0);
        conf.validate();
    }

}
