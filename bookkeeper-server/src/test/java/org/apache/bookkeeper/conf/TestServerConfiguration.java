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

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.Assert;
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

    @Test
    public void testCompactionSettings() throws ConfigurationException {
        ServerConfiguration conf = new ServerConfiguration();
        long major, minor;
        long entryLocationCompactionInterval;

        // Default Values
        major = conf.getMajorCompactionMaxTimeMillis();
        minor = conf.getMinorCompactionMaxTimeMillis();
        Assert.assertEquals(-1, major);
        Assert.assertEquals(-1, minor);

        // Set values major then minor
        conf.setMajorCompactionMaxTimeMillis(500).setMinorCompactionMaxTimeMillis(250);
        major = conf.getMajorCompactionMaxTimeMillis();
        minor = conf.getMinorCompactionMaxTimeMillis();
        Assert.assertEquals(500, major);
        Assert.assertEquals(250, minor);

        // Set values minor then major
        conf.setMinorCompactionMaxTimeMillis(150).setMajorCompactionMaxTimeMillis(1500);
        major = conf.getMajorCompactionMaxTimeMillis();
        minor = conf.getMinorCompactionMaxTimeMillis();
        Assert.assertEquals(1500, major);
        Assert.assertEquals(150, minor);

        // Default Values
        major = conf.getMajorCompactionInterval();
        minor = conf.getMinorCompactionInterval();
        Assert.assertEquals(3600, minor);
        Assert.assertEquals(86400, major);

        // Set values major then minor
        conf.setMajorCompactionInterval(43200).setMinorCompactionInterval(1800);
        major = conf.getMajorCompactionInterval();
        minor = conf.getMinorCompactionInterval();
        Assert.assertEquals(1800, minor);
        Assert.assertEquals(43200, major);

        // Set values minor then major
        conf.setMinorCompactionInterval(900).setMajorCompactionInterval(21700);
        major = conf.getMajorCompactionInterval();
        minor = conf.getMinorCompactionInterval();
        Assert.assertEquals(900, minor);
        Assert.assertEquals(21700, major);

        conf.setMinorCompactionInterval(500);
        try {
            conf.validate();
            fail();
        } catch (ConfigurationException ignore) {
        }

        conf.setMinorCompactionInterval(600);
        conf.validate();

        conf.setMajorCompactionInterval(550);
        try {
            conf.validate();
            fail();
        } catch (ConfigurationException ignore) {
        }

        conf.setMajorCompactionInterval(600);
        conf.validate();

        // Default Values
        double majorThreshold, minorThreshold;
        majorThreshold = conf.getMajorCompactionThreshold();
        minorThreshold = conf.getMinorCompactionThreshold();
        Assert.assertEquals(0.8, majorThreshold, 0.00001);
        Assert.assertEquals(0.2, minorThreshold, 0.00001);

        // Set values major then minor
        conf.setMajorCompactionThreshold(0.7).setMinorCompactionThreshold(0.1);
        majorThreshold = conf.getMajorCompactionThreshold();
        minorThreshold = conf.getMinorCompactionThreshold();
        Assert.assertEquals(0.7, majorThreshold, 0.00001);
        Assert.assertEquals(0.1, minorThreshold, 0.00001);

        // Set values minor then major
        conf.setMinorCompactionThreshold(0.3).setMajorCompactionThreshold(0.6);
        majorThreshold = conf.getMajorCompactionThreshold();
        minorThreshold = conf.getMinorCompactionThreshold();
        Assert.assertEquals(0.6, majorThreshold, 0.00001);
        Assert.assertEquals(0.3, minorThreshold, 0.00001);

        // Default Values
        entryLocationCompactionInterval = conf.getEntryLocationCompactionInterval();
        Assert.assertEquals(-1, entryLocationCompactionInterval);

        // Set entry location compaction
        conf.setEntryLocationCompactionInterval(3600);
        entryLocationCompactionInterval = conf.getEntryLocationCompactionInterval();
        Assert.assertEquals(3600, entryLocationCompactionInterval);

        conf.setEntryLocationCompactionInterval(550);
        try {
            conf.validate();
            fail();
        } catch (ConfigurationException ignore) {
        }

        conf.setEntryLocationCompactionInterval(650);
        conf.validate();
    }
}
