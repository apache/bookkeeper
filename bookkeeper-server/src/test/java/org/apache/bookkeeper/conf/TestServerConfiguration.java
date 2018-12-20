/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.conf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link ServerConfiguration}.
 */
public class TestServerConfiguration {

    private final ServerConfiguration serverConf;

    public TestServerConfiguration() {
        serverConf = new ServerConfiguration();
    }

    @Before
    public void setup() throws Exception {
        serverConf.loadConf(
            getClass().getClassLoader().getResource("bk_server.conf"));
    }

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

    @Test
    public void testSetExtraServerComponents() {
        ServerConfiguration conf = new ServerConfiguration();
        assertNull(conf.getExtraServerComponents());
        String[] components = new String[] {
            "test1", "test2", "test3"
        };
        conf.setExtraServerComponents(components);
        assertArrayEquals(components, conf.getExtraServerComponents());
    }

    @Test
    public void testGetExtraServerComponents() {
        String[] components = new String[] {
            "test1", "test2", "test3"
        };
        assertArrayEquals(components, serverConf.getExtraServerComponents());
    }

    @Test(expected = ConfigurationException.class)
    public void testMismatchofJournalAndFileInfoVersionsOlderJournalVersion() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalFormatVersionToWrite(5);
        conf.setFileInfoFormatVersionToWrite(1);
        conf.validate();
    }

    @Test(expected = ConfigurationException.class)
    public void testMismatchofJournalAndFileInfoVersionsOlderFileInfoVersion() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalFormatVersionToWrite(6);
        conf.setFileInfoFormatVersionToWrite(0);
        conf.validate();
    }

    @Test
    public void testValidityOfJournalAndFileInfoVersions() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setJournalFormatVersionToWrite(5);
        conf.setFileInfoFormatVersionToWrite(0);
        conf.validate();

        conf = new ServerConfiguration();
        conf.setJournalFormatVersionToWrite(6);
        conf.setFileInfoFormatVersionToWrite(1);
        conf.validate();
    }

    @Test
    public void testEntryLogSizeLimit() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        try {
            conf.setEntryLogSizeLimit(-1);
            fail("should fail setEntryLogSizeLimit since `logSizeLimit` is too small");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        try {
            conf.setProperty("logSizeLimit", "-1");
            conf.validate();
            fail("Invalid configuration since `logSizeLimit` is too small");
        } catch (ConfigurationException ce) {
            // expected
        }

        try {
            conf.setEntryLogSizeLimit(2 * 1024 * 1024 * 1024L - 1);
            fail("Should fail setEntryLogSizeLimit size `logSizeLimit` is too large");
        } catch (IllegalArgumentException iae) {
            // expected
        }
        try {
            conf.validate();
            fail("Invalid configuration since `logSizeLimit` is too large");
        } catch (ConfigurationException ce) {
            // expected
        }

        conf.setEntryLogSizeLimit(512 * 1024 * 1024);
        conf.validate();
        assertEquals(512 * 1024 * 1024, conf.getEntryLogSizeLimit());

        conf.setEntryLogSizeLimit(1073741824);
        conf.validate();
        assertEquals(1073741824, conf.getEntryLogSizeLimit());
    }
}
