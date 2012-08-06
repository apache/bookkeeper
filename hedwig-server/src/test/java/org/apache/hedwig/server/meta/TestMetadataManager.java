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
package org.apache.hedwig.server.meta;

import java.util.Map;
import java.util.concurrent.SynchronousQueue;

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.StubCallback;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRange;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionState;
import org.apache.hedwig.server.topics.HubInfo;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.util.Either;
import org.apache.hedwig.util.HedwigSocketAddress;

import org.junit.Test;
import org.junit.Assert;

public class TestMetadataManager extends MetadataManagerFactoryTestCase {

    public TestMetadataManager(String metadataManagerFactoryCls) {
        super(metadataManagerFactoryCls);
    }

    @Test
    public void testOwnerInfo() throws Exception {
        TopicOwnershipManager toManager = metadataManagerFactory.newTopicOwnershipManager();

        ByteString topic = ByteString.copyFromUtf8("testOwnerInfo");
        StubCallback<Versioned<HubInfo>> readCallback = new StubCallback<Versioned<HubInfo>>();
        StubCallback<Version> writeCallback = new StubCallback<Version>();
        StubCallback<Void> deleteCallback = new StubCallback<Void>();

        Either<Version, PubSubException> res;
        HubInfo owner = new HubInfo(new HedwigSocketAddress("127.0.0.1", 8008), 999);

        // Write non-existed owner info
        toManager.writeOwnerInfo(topic, owner, null, writeCallback, null);
        res = writeCallback.queue.take();
        Assert.assertEquals(null, res.right());
        Version v1 = res.left();

        // read owner info
        toManager.readOwnerInfo(topic, readCallback, null);
        Versioned<HubInfo> hubInfo = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v1.compare(hubInfo.getVersion()));
        Assert.assertEquals(owner, hubInfo.getValue());

        HubInfo newOwner = new HubInfo(new HedwigSocketAddress("127.0.0.1", 8008), 1000);

        // write exsited owner info with null version
        toManager.writeOwnerInfo(topic, newOwner, null, writeCallback, null);
        res = writeCallback.queue.take();
        Assert.assertNotNull(res.right());
        Assert.assertTrue(res.right() instanceof PubSubException.TopicOwnerInfoExistsException);

        // write existed owner info with right version
        toManager.writeOwnerInfo(topic, newOwner, v1, writeCallback, null);
        res = writeCallback.queue.take();
        Assert.assertEquals(null, res.right());
        Version v2 = res.left();
        Assert.assertEquals(Version.Occurred.AFTER, v2.compare(v1));

        // read owner info
        toManager.readOwnerInfo(topic, readCallback, null);
        hubInfo = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v2.compare(hubInfo.getVersion()));
        Assert.assertEquals(newOwner, hubInfo.getValue());

        HubInfo newOwner2 = new HubInfo(new HedwigSocketAddress("127.0.0.1", 8008), 1001);

        // write existed owner info with bad version
        toManager.writeOwnerInfo(topic, newOwner2, v1,
                                 writeCallback, null);
        res = writeCallback.queue.take();
        Assert.assertNotNull(res.right());
        Assert.assertTrue(res.right() instanceof PubSubException.BadVersionException);

        // read owner info
        toManager.readOwnerInfo(topic, readCallback, null);
        hubInfo = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v2.compare(hubInfo.getVersion()));
        Assert.assertEquals(newOwner, hubInfo.getValue());

        // delete existed owner info with bad version
        toManager.deleteOwnerInfo(topic, v1, deleteCallback, null);
        Assert.assertTrue(deleteCallback.queue.take().right() instanceof
                          PubSubException.BadVersionException);

        // read owner info
        toManager.readOwnerInfo(topic, readCallback, null);
        hubInfo = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v2.compare(hubInfo.getVersion()));

        // delete existed owner info with right version
        toManager.deleteOwnerInfo(topic, v2, deleteCallback, null);
        Assert.assertEquals(null, deleteCallback.queue.take().right());

        // Empty owner info
        toManager.readOwnerInfo(topic, readCallback, null);
        Assert.assertEquals(null, readCallback.queue.take().left());

        // delete non-existed owner info
        toManager.deleteOwnerInfo(topic, null, deleteCallback, null);
        Assert.assertTrue(deleteCallback.queue.take().right() instanceof
                          PubSubException.NoTopicOwnerInfoException);

        toManager.close();
    }

    @Test
    public void testPersistenceInfo() throws Exception {
        TopicPersistenceManager tpManager = metadataManagerFactory.newTopicPersistenceManager();

        ByteString topic = ByteString.copyFromUtf8("testPersistenceInfo");
        StubCallback<Versioned<LedgerRanges>> readCallback = new StubCallback<Versioned<LedgerRanges>>();
        StubCallback<Version> writeCallback = new StubCallback<Version>();
        StubCallback<Void> deleteCallback = new StubCallback<Void>();

        // Write non-existed persistence info
        tpManager.writeTopicPersistenceInfo(topic, LedgerRanges.getDefaultInstance(), null,
                                            writeCallback, null);
        Either<Version, PubSubException> res = writeCallback.queue.take();
        Assert.assertEquals(null, res.right());
        Version v1 = res.left();

        // read persistence info
        tpManager.readTopicPersistenceInfo(topic, readCallback, null);
        Versioned<LedgerRanges> ranges = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v1.compare(ranges.getVersion()));
        Assert.assertEquals(LedgerRanges.getDefaultInstance(), ranges.getValue());

        LedgerRange lastRange = LedgerRange.newBuilder().setLedgerId(1).build();
        LedgerRanges.Builder builder = LedgerRanges.newBuilder();
        builder.addRanges(lastRange);
        LedgerRanges newRanges = builder.build();

        // write existed persistence info with null version
        tpManager.writeTopicPersistenceInfo(topic, newRanges, null, writeCallback, null);
        res = writeCallback.queue.take();
        Assert.assertNotNull(res.right());
        Assert.assertTrue(res.right() instanceof PubSubException.TopicPersistenceInfoExistsException);

        // write existed persistence info with right version
        tpManager.writeTopicPersistenceInfo(topic, newRanges, v1,
                                            writeCallback, null);
        res = writeCallback.queue.take();
        Assert.assertEquals(null, res.right());
        Version v2 = res.left();
        Assert.assertEquals(Version.Occurred.AFTER, v2.compare(v1));

        // read persistence info
        tpManager.readTopicPersistenceInfo(topic, readCallback, null);
        ranges = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v2.compare(ranges.getVersion()));
        Assert.assertEquals(newRanges, ranges.getValue());

        lastRange = LedgerRange.newBuilder().setLedgerId(2).build();
        builder = LedgerRanges.newBuilder();
        builder.addRanges(lastRange);
        LedgerRanges newRanges2 = builder.build();

        // write existed persistence info with bad version
        tpManager.writeTopicPersistenceInfo(topic, newRanges2, v1,
                                            writeCallback, null);
        res = writeCallback.queue.take();
        Assert.assertNotNull(res.right());
        Assert.assertTrue(res.right() instanceof PubSubException.BadVersionException);

        // read persistence info
        tpManager.readTopicPersistenceInfo(topic, readCallback, null);
        ranges = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v2.compare(ranges.getVersion()));
        Assert.assertEquals(newRanges, ranges.getValue());

        // delete with bad version
        tpManager.deleteTopicPersistenceInfo(topic, v1, deleteCallback, null);
        Assert.assertTrue(deleteCallback.queue.take().right() instanceof
                          PubSubException.BadVersionException);

        // read persistence info
        tpManager.readTopicPersistenceInfo(topic, readCallback, null);
        ranges = readCallback.queue.take().left();
        Assert.assertEquals(Version.Occurred.CONCURRENTLY, v2.compare(ranges.getVersion()));
        Assert.assertEquals(newRanges, ranges.getValue());

        // delete existed persistence info with right version
        tpManager.deleteTopicPersistenceInfo(topic, v2, deleteCallback, null);
        Assert.assertEquals(null, deleteCallback.queue.take().right());

        // read empty persistence info
        tpManager.readTopicPersistenceInfo(topic, readCallback, null);
        Assert.assertEquals(null, readCallback.queue.take().left());

        // delete non-existed persistence info
        tpManager.deleteTopicPersistenceInfo(topic, null, deleteCallback, null);
        Assert.assertTrue(deleteCallback.queue.take().right() instanceof
                          PubSubException.NoTopicPersistenceInfoException);

        tpManager.close();
    }

    @Test
    public void testSubscriptionState() throws Exception {
        SubscriptionDataManager subManager = metadataManagerFactory.newSubscriptionDataManager();

        ByteString topic = ByteString.copyFromUtf8("testSubscriptionState");
        ByteString subid = ByteString.copyFromUtf8("mysub");

        StubCallback<Void> callback = new StubCallback<Void>();
        StubCallback<SubscriptionState> readCallback = new StubCallback<SubscriptionState>();
        StubCallback<Map<ByteString, InMemorySubscriptionState>> subsCallback
            = new StubCallback<Map<ByteString, InMemorySubscriptionState>>();

        subManager.readSubscriptionState(topic, subid, readCallback, null);
        Either<SubscriptionState, PubSubException> readRes = readCallback.queue.take();
        Assert.assertEquals("Found inconsistent subscription state", null, readRes.left());
        Assert.assertEquals("Should not fail with PubSubException", null, readRes.right());

        // read non-existed subscription state
        subManager.readSubscriptions(topic, subsCallback, null);
        Either<Map<ByteString, InMemorySubscriptionState>, PubSubException> res = subsCallback.queue.take();
        Assert.assertEquals("Found more than 0 subscribers", 0, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());

        // update non-existed subscription state
        subManager.updateSubscriptionState(topic, subid, SubscriptionState.getDefaultInstance(),
                                            callback, null);
        Assert.assertTrue("Should fail to update a non-existed subscriber with PubSubException",
                          callback.queue.take().right() instanceof PubSubException.NoSubscriptionStateException);

        // delete non-existed subscription state
        subManager.deleteSubscriptionState(topic, subid, callback, null);
        Assert.assertTrue("Should fail to delete a non-existed subscriber with PubSubException",
                          callback.queue.take().right() instanceof PubSubException.NoSubscriptionStateException);

        long seqId = 10;
        MessageSeqId.Builder builder = MessageSeqId.newBuilder();
        builder.setLocalComponent(seqId);
        MessageSeqId msgId = builder.build();

        SubscriptionState state = SubscriptionState.newBuilder(SubscriptionState.getDefaultInstance()).setMsgId(msgId).build();

        // create a subscription state
        subManager.createSubscriptionState(topic, subid, state, callback, null);
        Assert.assertEquals("Should not fail with PubSubException",
                            null, callback.queue.take().right());

        // read subscriptions
        subManager.readSubscriptions(topic, subsCallback, null);
        res = subsCallback.queue.take();
        Assert.assertEquals("Should find just 1 subscriber", 1, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());
        InMemorySubscriptionState imss = res.left().get(subid);
        Assert.assertEquals("Found inconsistent subscription state",
                            state, imss.getSubscriptionState());
        Assert.assertEquals("Found inconsistent last consumed seq id",
                            seqId, imss.getLastConsumeSeqId().getLocalComponent());

        // move consume seq id
        seqId = 99;
        builder = MessageSeqId.newBuilder();
        builder.setLocalComponent(seqId);
        msgId = builder.build();

        state = SubscriptionState.newBuilder(state).setMsgId(msgId).build();

        // update subscription state
        subManager.updateSubscriptionState(topic, subid, state, callback, null);
        Assert.assertEquals("Fail to update a subscription state", null, callback.queue.take().right());

        // read subscription state
        subManager.readSubscriptionState(topic, subid, readCallback, null);
        Assert.assertEquals("Found inconsistent subscription state",
                            state, readCallback.queue.take().left());

        // read subscriptions again
        subManager.readSubscriptions(topic, subsCallback, null);
        res = subsCallback.queue.take();
        Assert.assertEquals("Should find just 1 subscriber", 1, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());
        imss = res.left().get(subid);
        Assert.assertEquals("Found inconsistent subscription state",
                            state, imss.getSubscriptionState());
        Assert.assertEquals("Found inconsistent last consumed seq id",
                            seqId, imss.getLastConsumeSeqId().getLocalComponent());

        subManager.deleteSubscriptionState(topic, subid, callback, null);
        Assert.assertEquals("Fail to delete an existed subscriber", null, callback.queue.take().right());

        // read subscription states again
        subManager.readSubscriptions(topic, subsCallback, null);
        res = subsCallback.queue.take();
        Assert.assertEquals("Found more than 0 subscribers", 0, res.left().size());
        Assert.assertEquals("Should not fail with PubSubException", null, res.right());

        subManager.close();
    }
}
