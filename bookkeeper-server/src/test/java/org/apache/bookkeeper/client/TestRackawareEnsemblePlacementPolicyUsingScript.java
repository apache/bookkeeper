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
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.CommonConfigurationKeys;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetUtils;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.Shell;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In this testsuite, ScriptBasedMapping is used as DNS_RESOLVER_CLASS for
 * mapping nodes to racks. Shell Script -
 * src/test/resources/networkmappingscript.sh is used in ScriptBasedMapping for
 * resolving racks. This script maps HostAddress to rack depending on the last
 * character of the HostAddress string. for eg. 127.0.0.1 :- /1, 127.0.0.2 :-
 * /2, 99.12.34.21 :- /1
 *
 * <p>This testsuite has same testscenarios as in
 * TestRackawareEnsemblePlacementPolicy.java.
 *
 * <p>For now this Testsuite works only on Unix based OS.
 */
public class TestRackawareEnsemblePlacementPolicyUsingScript {

    static final Logger LOG = LoggerFactory.getLogger(TestRackawareEnsemblePlacementPolicyUsingScript.class);

    HashedWheelTimer timer;
    RackawareEnsemblePlacementPolicy repp;
    ClientConfiguration conf = new ClientConfiguration();

    @Before
    public void setUp() throws Exception {
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
        conf.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                "src/test/resources/networkmappingscript.sh");
        timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(conf, Optional.<DNSToSwitchMapping>empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
    }

    @After
    public void tearDown() throws Exception {
        repp.uninitalize();
    }

    private void ignoreTestIfItIsWindowsOS() {
        Assume.assumeTrue(!Shell.WINDOWS);
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInSameRack() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.1.2", 3181); // /2 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // replace node under r2
        BookieId replacedBookie = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(),
                                                                addr2.toBookieId(), new HashSet<>()).getResult();
        assertEquals(addr3.toBookieId(), replacedBookie);
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInDifferentRack() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181); // /3 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // replace node under r2
        Set<BookieId> excludedAddrs = new HashSet<BookieId>();
        excludedAddrs.add(addr1.toBookieId());
        BookieId replacedBookie = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(),
                                                                addr2.toBookieId(), excludedAddrs).getResult();

        assertFalse(addr1.toBookieId().equals(replacedBookie));
        assertTrue(addr3.toBookieId().equals(replacedBookie)
                || addr4.toBookieId().equals(replacedBookie));
    }

    @Test
    public void testReplaceBookieWithNotEnoughBookies() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181); // /3 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack

        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        // replace node under r2
        Set<BookieId> excludedAddrs = new HashSet<BookieId>();
        excludedAddrs.add(addr1.toBookieId());
        excludedAddrs.add(addr3.toBookieId());
        excludedAddrs.add(addr4.toBookieId());
        try {
            repp.replaceBookie(1, 1, 1, null, new ArrayList<BookieId>(), addr2.toBookieId(), excludedAddrs);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not BKNotEnoughBookiesException
        }
    }

    /*
     * Test that even in case of script mapping error
     * we are getting default rack that makes sense for the policy.
     * i.e. if all nodes in rack-aware policy use /rack format
     * but one gets node /default-region/default-rack the node addition to topology will fail.
     *
     * This case adds node with non-default rack, then adds nodes with one on default rack.
     */
    @Test
    public void testReplaceBookieWithScriptMappingError() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr0 = new BookieSocketAddress("127.0.0.0", 3181); // error mapping to rack here
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack

        // Update cluster, add node that maps to non-default rack
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());

        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        addrs = new HashSet<BookieId>();
        addrs.add(addr0.toBookieId());
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        // replace node under r2
        Set<BookieId> excludedAddrs = new HashSet<BookieId>();
        excludedAddrs.add(addr1.toBookieId());
        BookieId replacedBookie = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(),
                                                                addr2.toBookieId(), excludedAddrs).getResult();

        assertFalse(addr1.toBookieId().equals(replacedBookie));
        assertFalse(addr2.toBookieId().equals(replacedBookie));
        assertTrue(addr0.toBookieId().equals(replacedBookie));
    }

    /*
     * Test that even in case of script mapping error
     * we are getting default rack that makes sense for the policy.
     * i.e. if all nodes in rack-aware policy use /rack format
     * but one gets node /default-region/default-rack the node addition to topology will fail.
     *
     * This case adds node with default rack, then adds nodes with non-default rack.
     * Almost the same as testReplaceBookieWithScriptMappingError but different order of addition.
     */
    @Test
    public void testReplaceBookieWithScriptMappingError2() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr0 = new BookieSocketAddress("127.0.0.0", 3181); // error mapping to rack here
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack

        // Update cluster, add node that maps to default rack first
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr0.toBookieId());

        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        addrs = new HashSet<BookieId>();
        addrs.add(addr0.toBookieId());
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());

        // replace node under r2
        Set<BookieId> excludedAddrs = new HashSet<BookieId>();
        excludedAddrs.add(addr1.toBookieId());
        BookieId replacedBookie = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(),
                                                                addr2.toBookieId(), excludedAddrs).getResult();

        assertFalse(addr1.toBookieId().equals(replacedBookie));
        assertFalse(addr2.toBookieId().equals(replacedBookie));
        assertTrue(addr0.toBookieId().equals(replacedBookie));
    }

    @Test
    public void testNewEnsembleWithSingleRack() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.1.1", 3181); // /1 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.2.1", 3181); // /1 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.3.1", 3181); // /1 rack
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        try {
            List<BookieId> ensemble = repp.newEnsemble(3, 2, 2, null,
                                                                  new HashSet<>()).getResult();
            assertEquals(0, getNumCoveredWriteQuorums(ensemble, 2));
            List<BookieId> ensemble2 = repp.newEnsemble(4, 2, 2, null,
                                                                   new HashSet<>()).getResult();
            assertEquals(0, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleWithMultipleRacks() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.1.2", 3181); // /2 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.2.2", 3181); // /2 rack
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        try {
            List<BookieId> ensemble = repp.newEnsemble(3, 2, 2, null,
                                                                  new HashSet<>()).getResult();
            int numCovered = getNumCoveredWriteQuorums(ensemble, 2);
            assertTrue(numCovered == 2);
            List<BookieId> ensemble2 = repp.newEnsemble(4, 2, 2, null,
                                                                   new HashSet<>()).getResult();
            numCovered = getNumCoveredWriteQuorums(ensemble2, 2);
            assertTrue(numCovered == 2);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception");
        }
    }

    @Test
    public void testNewEnsembleWithEnoughRacks() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181); // /3 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack
        BookieSocketAddress addr5 = new BookieSocketAddress("127.0.1.1", 3181); // /1 rack
        BookieSocketAddress addr6 = new BookieSocketAddress("127.0.1.2", 3181); // /2 rack
        BookieSocketAddress addr7 = new BookieSocketAddress("127.0.1.3", 3181); // /3 rack
        BookieSocketAddress addr8 = new BookieSocketAddress("127.0.1.4", 3181); // /4 rack
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        addrs.add(addr5.toBookieId());
        addrs.add(addr6.toBookieId());
        addrs.add(addr7.toBookieId());
        addrs.add(addr8.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        try {
            List<BookieId> ensemble1 = repp.newEnsemble(3, 2, 2, null,
                                                                   new HashSet<>()).getResult();
            assertEquals(3, getNumCoveredWriteQuorums(ensemble1, 2));
            List<BookieId> ensemble2 = repp.newEnsemble(4, 2, 2, null,
                                                                   new HashSet<>()).getResult();
            assertEquals(4, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception.");
        }
    }

    /**
     * Test for BOOKKEEPER-633.
     */

    @Test
    public void testRemoveBookieFromCluster() {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.1.2", 3181); // /2 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack
        // Update cluster
        Set<BookieId> addrs = new HashSet<BookieId>();
        addrs.add(addr1.toBookieId());
        addrs.add(addr2.toBookieId());
        addrs.add(addr3.toBookieId());
        addrs.add(addr4.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
        addrs.remove(addr1.toBookieId());
        repp.onClusterChanged(addrs, new HashSet<BookieId>());
    }

    @Test
    public void testNetworkTopologyScriptFileNameIsEmpty() throws Exception {
        ignoreTestIfItIsWindowsOS();
        repp.uninitalize();

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.setProperty(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
        newConf.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY, "");
        newConf.setEnforceMinNumRacksPerWriteQuorum(false);
        timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                newConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS, newConf.getTimeoutTimerNumTicks());

        repp = new RackawareEnsemblePlacementPolicy();
        try {
            repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        } catch (RuntimeException re) {
            fail("EnforceMinNumRacksPerWriteQuorum is not set, so repp.initialize should succeed even if"
                    + " networkTopologyScriptFileName is empty");
        }
        repp.uninitalize();

        newConf.setEnforceMinNumRacksPerWriteQuorum(true);
        repp = new RackawareEnsemblePlacementPolicy();
        try {
            repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
            fail("EnforceMinNumRacksPerWriteQuorum is set, so repp.initialize should fail if"
                    + " networkTopologyScriptFileName is empty");
        } catch (RuntimeException re) {
        }
        repp.uninitalize();

        newConf.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                "src/test/resources/networkmappingscript.sh");
        try {
            repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        } catch (RuntimeException re) {
            fail("EnforceMinNumRacksPerWriteQuorum is set and networkTopologyScriptFileName is not empty,"
                    + " so it should succeed");
        }
        repp.uninitalize();
    }

    @Test
    public void testIfValidateConfFails() throws Exception {
        ignoreTestIfItIsWindowsOS();
        repp.uninitalize();

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.setProperty(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
        /*
         * this script, exits with error value if no argument is passed to it.
         * So mapping.validateConf will fail.
         */
        newConf.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                "src/test/resources/networkmappingscriptwithargs.sh");
        timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                newConf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS, newConf.getTimeoutTimerNumTicks());

        repp = new RackawareEnsemblePlacementPolicy();
        repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer,
                DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

        repp.uninitalize();
        repp = new RackawareEnsemblePlacementPolicy();
        try {
            repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        } catch (RuntimeException re) {
            fail("EnforceMinNumRacksPerWriteQuorum is not set, so repp.initialize should succeed"
                    + " even if mapping.validateConf fails");
        }

        newConf.setEnforceMinNumRacksPerWriteQuorum(true);
        repp.uninitalize();
        repp = new RackawareEnsemblePlacementPolicy();
        try {
            repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
            fail("EnforceMinNumRacksPerWriteQuorum is set, so repp.initialize should fail"
                    + " if mapping.validateConf fails");
        } catch (RuntimeException re) {

        }

        /*
         * this script returns successfully even if no argument is passed to it.
         * So mapping.validateConf will succeed.
         */
        newConf.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                "src/test/resources/networkmappingscript.sh");
        repp.uninitalize();
        repp = new RackawareEnsemblePlacementPolicy();
        try {
            repp.initialize(newConf, Optional.<DNSToSwitchMapping> empty(), timer, DISABLE_ALL,
                    NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        } catch (RuntimeException re) {
            fail("EnforceMinNumRacksPerWriteQuorum is set, and mapping.validateConf succeeds."
                    + " So repp.initialize should succeed");
        }
    }

    @Test
    public void testUseHostnameArgsOption() throws Exception {
        ignoreTestIfItIsWindowsOS();
        repp.uninitalize();

        // Mock NetUtils.
        try (MockedStatic<NetUtils> netUtils = Mockito.mockStatic(NetUtils.class)) {
            netUtils.when(() -> NetUtils.resolveNetworkLocation(
                    any(DNSToSwitchMapping.class), any(BookieSocketAddress.class))).thenCallRealMethod();
            netUtils.when(() -> NetUtils.normalizeToHostNames(any())).thenCallRealMethod();
            netUtils.when(() -> NetUtils.normalizeToHostName(anyString())).thenCallRealMethod();

            netUtils.when(() -> NetUtils.normalizeToHostName(
                    eq(InetAddress.getLocalHost().getHostAddress()))).thenReturn("bookie1");
            netUtils.when(() -> NetUtils.normalizeToHostName(eq("127.0.0.4"))).thenReturn("bookie22");

            // Initialize RackawareEnsemblePlacementPolicy.
            ClientConfiguration newConf = new ClientConfiguration();
            newConf.setProperty(REPP_DNS_RESOLVER_CLASS, ScriptBasedMapping.class.getName());
            newConf.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
                    "src/test/resources/networkmappingscript.sh");
            newConf.setProperty(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_USE_HOSTNAME_ARGS_KEY, true);
            timer = new HashedWheelTimer(
                    new ThreadFactoryBuilder().setNameFormat("TestTimer-%d").build(),
                    conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                    conf.getTimeoutTimerNumTicks());
            repp = new RackawareEnsemblePlacementPolicy();
            repp.initialize(newConf, Optional.<DNSToSwitchMapping>empty(), timer,
                    DISABLE_ALL, NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);

            // Join Bookie2, Bookie22, Bookie3, and Bookie33.
            BookieSocketAddress addr1 = new BookieSocketAddress("bookie2", 3181);   // /2 rack
            BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.4", 3181); // /2 rack
            BookieSocketAddress addr3 = new BookieSocketAddress("bookie3", 3181);   // /3 rack
            BookieSocketAddress addr4 = new BookieSocketAddress("bookie33", 3181);  // /3 rack
            Set<BookieId> addrs = new HashSet<>();
            addrs.add(addr1.toBookieId());
            addrs.add(addr2.toBookieId());
            addrs.add(addr3.toBookieId());
            addrs.add(addr4.toBookieId());
            repp.onClusterChanged(addrs, new HashSet<>());

            // Remove Bookie2.
            addrs.remove(addr1.toBookieId());
            repp.onClusterChanged(addrs, new HashSet<>());

            BookieId replacedBookie = repp.replaceBookie(1, 1, 1, null, new ArrayList<>(),
                    addr1.toBookieId(), new HashSet<>()).getResult();
            assertEquals(addr2.toBookieId(), replacedBookie);

            netUtils.verify(() -> NetUtils.normalizeToHostName(
                    eq(InetAddress.getLocalHost().getHostAddress())), times(1));
            netUtils.verify(() -> NetUtils.normalizeToHostName(eq("127.0.0.4")), times(1));
        }
    }

    private int getNumCoveredWriteQuorums(List<BookieId> ensemble, int writeQuorumSize)
            throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> racks = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                BookieId addr = ensemble.get(bookieIdx);
                String hostAddress = repp.bookieAddressResolver.resolve(addr)
                                            .getSocketAddress().getAddress().getHostAddress();
                String rack = "/" + hostAddress.charAt(hostAddress.length() - 1);
                racks.add(rack);
            }
            numCoveredWriteQuorums += (racks.size() > 1 ? 1 : 0);
        }
        return numCoveredWriteQuorums;
    }

}
