package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.CommonConfigurationKeys;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.ScriptBasedMapping;
import org.apache.bookkeeper.util.Shell;
import org.jboss.netty.util.HashedWheelTimer;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * In this testsuite, ScriptBasedMapping is used as DNS_RESOLVER_CLASS for
 * mapping nodes to racks. Shell Script -
 * src/test/resources/networkmappingscript.sh is used in ScriptBasedMapping for
 * resolving racks. This script maps HostAddress to rack depending on the last
 * character of the HostAddress string. for eg. 127.0.0.1 :- /1, 127.0.0.2 :-
 * /2, 99.12.34.21 :- /1
 * 
 * This testsuite has same testscenarios as in
 * TestRackawareEnsemblePlacementPolicy.java.
 * 
 * For now this Testsuite works only on Unix based OS.
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
        repp.initialize(conf, Optional.<DNSToSwitchMapping>absent(), timer, DISABLE_ALL, null);
    }

    @After
    public void tearDown() throws Exception {
        repp.uninitalize();
    }

    private void ignoreTestIfItIsWindowsOS() {
        Assume.assumeTrue(!Shell.WINDOWS);
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithEnoughBookiesInSameRack() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.1.2", 3181); // /2 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        BookieSocketAddress replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<BookieSocketAddress>(), addr2, new HashSet<BookieSocketAddress>());
        assertEquals(addr3, replacedBookie);
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithEnoughBookiesInDifferentRack() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181); // /3 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
        excludedAddrs.add(addr1);
        BookieSocketAddress replacedBookie = repp.replaceBookie(1, 1, 1, null, new HashSet<BookieSocketAddress>(), addr2, excludedAddrs);

        assertFalse(addr1.equals(replacedBookie));
        assertTrue(addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
    }

    @Test(timeout = 60000)
    public void testReplaceBookieWithNotEnoughBookies() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.0.3", 3181); // /3 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack

        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        // replace node under r2
        Set<BookieSocketAddress> excludedAddrs = new HashSet<BookieSocketAddress>();
        excludedAddrs.add(addr1);
        excludedAddrs.add(addr3);
        excludedAddrs.add(addr4);
        try {
            repp.replaceBookie(1, 1, 1, null, new HashSet<BookieSocketAddress>(), addr2, excludedAddrs);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not BKNotEnoughBookiesException
        }
    }

    @Test(timeout = 60000)
    public void testNewEnsembleWithSingleRack() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.1.1", 3181); // /1 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.2.1", 3181); // /1 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.3.1", 3181); // /1 rack
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(0, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test(timeout = 60000)
    public void testNewEnsembleWithMultipleRacks() throws Exception {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.1.2", 3181); // /2 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.2.2", 3181); // /2 rack
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            ArrayList<BookieSocketAddress> ensemble = repp.newEnsemble(3, 2, 2, null, new HashSet<BookieSocketAddress>());
            int numCovered = getNumCoveredWriteQuorums(ensemble, 2);
            assertTrue(numCovered == 2);
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<BookieSocketAddress>());
            numCovered = getNumCoveredWriteQuorums(ensemble2, 2);
            assertTrue(numCovered == 2);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception");
        }
    }

    @Test(timeout = 90000)
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
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        try {
            ArrayList<BookieSocketAddress> ensemble1 = repp.newEnsemble(3, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(3, getNumCoveredWriteQuorums(ensemble1, 2));
            ArrayList<BookieSocketAddress> ensemble2 = repp.newEnsemble(4, 2, 2, null, new HashSet<BookieSocketAddress>());
            assertEquals(4, getNumCoveredWriteQuorums(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception.");
        }
    }

    /**
     * Test for BOOKKEEPER-633
     */

    @Test(timeout = 60000)
    public void testRemoveBookieFromCluster() {
        ignoreTestIfItIsWindowsOS();
        BookieSocketAddress addr1 = new BookieSocketAddress("127.0.0.1", 3181); // /1 rack
        BookieSocketAddress addr2 = new BookieSocketAddress("127.0.0.2", 3181); // /2 rack
        BookieSocketAddress addr3 = new BookieSocketAddress("127.0.1.2", 3181); // /2 rack
        BookieSocketAddress addr4 = new BookieSocketAddress("127.0.0.4", 3181); // /4 rack
        // Update cluster
        Set<BookieSocketAddress> addrs = new HashSet<BookieSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
        addrs.remove(addr1);
        repp.onClusterChanged(addrs, new HashSet<BookieSocketAddress>());
    }

    private int getNumCoveredWriteQuorums(ArrayList<BookieSocketAddress> ensemble, int writeQuorumSize)
            throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> racks = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                BookieSocketAddress addr = ensemble.get(bookieIdx);
                String hostAddress = addr.getSocketAddress().getAddress().getHostAddress();
                String rack = "/" + hostAddress.charAt(hostAddress.length() - 1);
                racks.add(rack);
            }
            numCoveredWriteQuorums += (racks.size() > 1 ? 1 : 0);
        }
        return numCoveredWriteQuorums;
    }
    
}
