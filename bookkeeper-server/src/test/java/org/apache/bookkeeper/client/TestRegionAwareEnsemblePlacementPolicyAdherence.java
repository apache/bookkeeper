/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_MINIMUM_REGIONS_FOR_DURABILITY;
import static org.apache.bookkeeper.client.RegionAwareEnsemblePlacementPolicy.REPP_REGIONS_TO_WRITE;
import static org.apache.bookkeeper.feature.SettableFeatureProvider.DISABLE_ALL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.util.HashedWheelTimer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementPolicyAdherence;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy.PlacementResult;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.feature.FeatureProvider;
import org.apache.bookkeeper.feature.SettableFeature;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RegionAwareEnsemblePlacementPolicy#isEnsembleAdheringToPlacementPolicy} and
 * {@link RegionAwareEnsemblePlacementPolicy#replaceToAdherePlacementPolicy}: unresolved regions, repair of a
 * region-only violation, and no address resolution for bookies that left the cluster.
 */
public class TestRegionAwareEnsemblePlacementPolicyAdherence {

    private final ClientConfiguration conf = new ClientConfiguration();
    private HashedWheelTimer timer;
    private RegionAwareEnsemblePlacementPolicy repp;

    @BeforeEach
    void setUp() {
        StaticDNSResolver.reset();
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("T-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS, conf.getTimeoutTimerNumTicks());
    }

    @AfterEach
    void tearDown() {
        if (repp != null) {
            repp.uninitalize();
        }
        timer.stop();
    }

    private BookieId bookie(String ip, String rack) throws Exception {
        BookieSocketAddress a = new BookieSocketAddress(ip, 3181);
        StaticDNSResolver.addNodeToRack(a.getHostName(), rack);
        return a.toBookieId();
    }

    private void init(ClientConfiguration c, List<BookieId> bookies) {
        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(c, Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.onClusterChanged(new HashSet<>(bookies), new HashSet<BookieId>());
    }

    private void init(ClientConfiguration c, FeatureProvider featureProvider, Set<BookieId> writable,
            Set<BookieId> readOnly) {
        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(c, Optional.<DNSToSwitchMapping>empty(), timer, featureProvider,
                NullStatsLogger.INSTANCE, BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER);
        repp.onClusterChanged(writable, readOnly);
    }

    private ClientConfiguration durabilityConf() {
        ClientConfiguration c = new ClientConfiguration(conf);
        c.setProperty(REPP_REGIONS_TO_WRITE, "region-1;region-2");
        c.setProperty(REPP_MINIMUM_REGIONS_FOR_DURABILITY, 2);
        return c;
    }

    /** a bookie whose location is unresolved ("UnknownRegion") must not count as an independent region. */
    @Test
    public void testUnmappedBookieIsNotCountedAsSeparateRegion() throws Exception {
        BookieId mapped = bookie("127.0.0.12", "/region-1/az-a");
        BookieId unmapped = bookie("127.0.0.13", NetworkTopology.DEFAULT_REGION_AND_RACK);
        BookieId mapped2 = bookie("127.0.0.14", "/region-2/az-a");
        init(durabilityConf(), Lists.newArrayList(mapped, unmapped, mapped2));

        // one mapped + one unmapped replica: the unmapped one may live in the same region as the mapped one
        assertNotEquals(PlacementPolicyAdherence.MEETS_STRICT,
                repp.isEnsembleAdheringToPlacementPolicy(Lists.newArrayList(mapped, unmapped), 2, 2),
                "mapped + unmapped replica must not be graded strictly durable");
    }

    /** Control: two properly mapped regions are strictly durable. */
    @Test
    public void testTwoMappedRegionsAreStrict() throws Exception {
        BookieId b1 = bookie("127.0.0.12", "/region-1/az-a");
        BookieId b2 = bookie("127.0.0.14", "/region-2/az-a");
        init(durabilityConf(), Lists.newArrayList(b1, b2));
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT,
                repp.isEnsembleAdheringToPlacementPolicy(Lists.newArrayList(b1, b2), 2, 2));
    }

    /** repair must be able to fix an ensemble flagged FAIL purely by the region/durability rule. */
    @Test
    public void testReplaceToAdhereFixesDurabilityOnlyViolation() throws Exception {
        BookieId r1a = bookie("127.0.0.12", "/region-1/az-a");
        BookieId r1b = bookie("127.0.0.13", "/region-1/az-b");
        BookieId r2a = bookie("127.0.0.15", "/region-2/az-a");
        BookieId r2b = bookie("127.0.0.16", "/region-2/az-b");
        init(durabilityConf(), Lists.newArrayList(r1a, r1b, r2a, r2b));

        // wq=2/ack=2, both replicas in region-1 on different racks: losing region-1 loses both replicas.
        List<BookieId> current = Lists.newArrayList(r1a, r1b);
        assertEquals(PlacementPolicyAdherence.FAIL,
                repp.isEnsembleAdheringToPlacementPolicy(current, 2, 2), "precondition: flagged FAIL");

        // a single-bookie move to region-2 fixes it, so repair must find a target
        PlacementResult<List<BookieId>> result = repp.replaceToAdherePlacementPolicy(
                2, 2, 2, new HashSet<BookieId>(), current);
        assertNotEquals(PlacementPolicyAdherence.FAIL, result.getAdheringToPolicy(),
                "flagged ensemble can never be repaired: " + result.getResult());
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT,
                repp.isEnsembleAdheringToPlacementPolicy(result.getResult(), 2, 2));
    }

    /** an ensemble whose only violation is unsatisfiable (wq=3/ack=2, 2 regions) has no repair. */
    @Test
    public void testReplaceToAdhereReturnsFailWhenNoValidTargetExists() throws Exception {
        BookieId r1a = bookie("127.0.0.12", "/region-1/az-a");
        BookieId r1b = bookie("127.0.0.13", "/region-1/az-b");
        BookieId r1c = bookie("127.0.0.14", "/region-1/az-c");
        BookieId r2a = bookie("127.0.0.15", "/region-2/az-a");
        init(durabilityConf(), Lists.newArrayList(r1a, r1b, r1c, r2a));
        PlacementResult<List<BookieId>> result = repp.replaceToAdherePlacementPolicy(
                3, 3, 2, new HashSet<BookieId>(), Lists.newArrayList(r1a, r1b, r1c));
        assertEquals(PlacementPolicyAdherence.FAIL, result.getAdheringToPolicy());
    }

    /** departed (not known) bookies must not trigger address resolution per quorum per ledger. */
    @Test
    public void testDepartedBookieDoesNotResolveAddressOnEveryCheck() throws Exception {
        BookieId b1 = bookie("127.0.0.12", "/region-1/az-a");
        BookieId b2 = bookie("127.0.0.13", "/region-2/az-a");
        BookieId gone = bookie("127.0.0.14", "/region-1/az-b");
        AtomicInteger resolveCalls = new AtomicInteger();
        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(durabilityConf(), Optional.<DNSToSwitchMapping>empty(), timer, DISABLE_ALL,
                NullStatsLogger.INSTANCE, id -> {
                    resolveCalls.incrementAndGet();
                    return BookieSocketAddress.LEGACY_BOOKIEID_RESOLVER.resolve(id);
                });
        Set<BookieId> up = new HashSet<>(Lists.newArrayList(b1, b2, gone));
        repp.onClusterChanged(up, new HashSet<BookieId>());
        Set<BookieId> down = new HashSet<>(Lists.newArrayList(gone));
        repp.onClusterChanged(new HashSet<>(Lists.newArrayList(b1, b2)), down); // 'gone' leaves; cache evicted

        resolveCalls.set(0);
        int ledgers = 100;
        for (int i = 0; i < ledgers; i++) {
            repp.isEnsembleAdheringToPlacementPolicy(Lists.newArrayList(b1, b2, gone), 3, 2);
        }
        assertEquals(0, resolveCalls.get(), "address resolutions performed by the adherence check for " + ledgers
                + " ledgers");
    }

    /** all-unresolved bookies must not grade better than FAIL. */
    @Test
    public void testAllUnmappedBookiesAreGradedFail() throws Exception {
        BookieId u1 = bookie("127.0.0.12", NetworkTopology.DEFAULT_REGION_AND_RACK);
        BookieId u2 = bookie("127.0.0.13", NetworkTopology.DEFAULT_REGION_AND_RACK);
        BookieId m = bookie("127.0.0.14", "/region-1/az-a");
        init(durabilityConf(), Lists.newArrayList(u1, u2, m));
        assertEquals(PlacementPolicyAdherence.FAIL,
                repp.isEnsembleAdheringToPlacementPolicy(Lists.newArrayList(u1, u2), 2, 2));
    }

    /** a read-only bookie keeps its region: it still holds replicas, so it must not be treated as unresolved. */
    @Test
    public void testReadOnlyBookieKeepsItsRegion() throws Exception {
        BookieId writable = bookie("127.0.0.12", "/region-1/az-a");
        BookieId readOnly = bookie("127.0.0.13", "/region-2/az-a");
        Set<BookieId> both = new HashSet<>(Lists.newArrayList(writable, readOnly));
        init(durabilityConf(), DISABLE_ALL, both, new HashSet<BookieId>());
        List<BookieId> ensemble = Lists.newArrayList(writable, readOnly);
        assertEquals(PlacementPolicyAdherence.MEETS_STRICT, repp.isEnsembleAdheringToPlacementPolicy(ensemble, 2, 2));

        // the second bookie turns read-only: it leaves the writable set
        Set<BookieId> onlyWritable = new HashSet<>(Lists.newArrayList(writable));
        repp.onClusterChanged(onlyWritable, new HashSet<>(Lists.newArrayList(readOnly)));
        // not writable, so the rack analysis ignores it and the grade is SOFT, but it must not be flagged
        assertNotEquals(PlacementPolicyAdherence.FAIL, repp.isEnsembleAdheringToPlacementPolicy(ensemble, 2, 2),
                "an ensemble with a read-only bookie in another region must not be flagged");

        // once it is gone for good, its region is no longer known: unresolved, so not durable
        repp.onClusterChanged(onlyWritable, new HashSet<BookieId>());
        assertEquals(PlacementPolicyAdherence.FAIL, repp.isEnsembleAdheringToPlacementPolicy(ensemble, 2, 2));
    }

    /** a read-only bookie that starts read-only (never writable for this client) is resolved once, at cluster change. */
    @Test
    public void testBookieThatIsReadOnlyFromTheStartKeepsItsRegion() throws Exception {
        BookieId writable = bookie("127.0.0.12", "/region-1/az-a");
        BookieId readOnly = bookie("127.0.0.13", "/region-2/az-a");
        init(durabilityConf(), DISABLE_ALL, new HashSet<>(Lists.newArrayList(writable)),
                new HashSet<>(Lists.newArrayList(readOnly)));
        assertNotEquals(PlacementPolicyAdherence.FAIL,
                repp.isEnsembleAdheringToPlacementPolicy(Lists.newArrayList(writable, readOnly), 2, 2));
    }

    /** repair must not move a replica into a region where placement is disallowed by the feature flag. */
    @Test
    public void testReplaceToAdhereSkipsDisallowedRegion() throws Exception {
        FeatureProvider featureProvider = new SettableFeatureProvider("", 0);
        ClientConfiguration c = durabilityConf();
        c.setProperty(REPP_DISALLOW_BOOKIE_PLACEMENT_IN_REGION_FEATURE_NAME, "disallowBookies");
        BookieId r1a = bookie("127.0.0.12", "/region-1/az-a");
        BookieId r1b = bookie("127.0.0.13", "/region-1/az-b");
        BookieId r2a = bookie("127.0.0.15", "/region-2/az-a");
        init(c, featureProvider, new HashSet<>(Lists.newArrayList(r1a, r1b, r2a)), new HashSet<BookieId>());
        List<BookieId> current = Lists.newArrayList(r1a, r1b);

        // control: while region-2 is allowed, the single valid move goes there
        PlacementResult<List<BookieId>> allowed = repp.replaceToAdherePlacementPolicy(
                2, 2, 2, new HashSet<BookieId>(), current);
        assertTrue(allowed.getResult().contains(r2a), "control: " + allowed.getResult());

        ((SettableFeature) featureProvider.scope("region-2").getFeature("disallowBookies")).set(true);
        PlacementResult<List<BookieId>> disallowed = repp.replaceToAdherePlacementPolicy(
                2, 2, 2, new HashSet<BookieId>(), current);
        assertFalse(disallowed.getResult().contains(r2a),
                "repair placed a replica in a disallowed region: " + disallowed.getResult());
        assertEquals(PlacementPolicyAdherence.FAIL, disallowed.getAdheringToPolicy());
    }

    /** repairs must be spread over the valid targets, not always land on the same bookie. */
    @Test
    public void testReplaceToAdhereSpreadsTargetsAcrossValidCandidates() throws Exception {
        BookieId r1a = bookie("127.0.0.12", "/region-1/az-a");
        BookieId r1b = bookie("127.0.0.13", "/region-1/az-b");
        Set<BookieId> all = new HashSet<>(Lists.newArrayList(r1a, r1b));
        for (int i = 0; i < 10; i++) {
            all.add(bookie("127.0.1." + (10 + i), "/region-2/az-" + i));
        }
        init(durabilityConf(), DISABLE_ALL, all, new HashSet<BookieId>());

        Set<BookieId> targets = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            PlacementResult<List<BookieId>> result = repp.replaceToAdherePlacementPolicy(
                    2, 2, 2, new HashSet<BookieId>(), Lists.newArrayList(r1a, r1b));
            // every choice is valid by itself
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT, result.getAdheringToPolicy());
            assertEquals(PlacementPolicyAdherence.MEETS_STRICT,
                    repp.isEnsembleAdheringToPlacementPolicy(result.getResult(), 2, 2));
            targets.addAll(result.getResult());
        }
        targets.remove(r1a);
        targets.remove(r1b);
        assertTrue(targets.size() > 1, "200 repairs all chose " + targets + " out of 10 equally valid bookies");
    }
}
