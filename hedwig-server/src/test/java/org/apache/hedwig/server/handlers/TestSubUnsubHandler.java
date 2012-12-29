/**
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
package org.apache.hedwig.server.handlers;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Test;

import com.google.protobuf.ByteString;
import org.apache.hedwig.StubCallback;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.filter.PipelineFilter;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ProtocolVersion;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.UnsubscribeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.delivery.ChannelEndPoint;
import org.apache.hedwig.server.delivery.StubDeliveryManager;
import org.apache.hedwig.server.delivery.StubDeliveryManager.StartServingRequest;
import org.apache.hedwig.server.netty.WriteRecordingChannel;
import org.apache.hedwig.server.persistence.LocalDBPersistenceManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.subscriptions.AllToAllTopologyFilter;
import org.apache.hedwig.server.subscriptions.StubSubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.apache.hedwig.util.ConcurrencyUtils;

import junit.framework.TestCase;

public class TestSubUnsubHandler extends TestCase {

    SubscribeHandler sh;
    StubDeliveryManager dm;
    StubSubscriptionManager sm;
    SubscriptionChannelManager subChannelMgr;
    ByteString topic = ByteString.copyFromUtf8("topic");
    WriteRecordingChannel channel;

    SubscribeRequest subRequestPrototype;
    PubSubRequest pubSubRequestPrototype;
    ByteString subscriberId;
    UnsubscribeHandler ush;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        ServerConfiguration conf = new ServerConfiguration();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        TopicManager tm = new TrivialOwnAllTopicManager(conf, executor);
        dm = new StubDeliveryManager();
        PersistenceManager pm = LocalDBPersistenceManager.instance();
        sm = new StubSubscriptionManager(tm, pm, dm, conf, executor);
        subChannelMgr = new SubscriptionChannelManager();
        sh = new SubscribeHandler(conf, tm, dm, pm, sm, subChannelMgr);
        channel = new WriteRecordingChannel();

        subscriberId = ByteString.copyFromUtf8("subId");

        subRequestPrototype = SubscribeRequest.newBuilder().setSubscriberId(subscriberId).build();
        pubSubRequestPrototype = PubSubRequest.newBuilder().setProtocolVersion(ProtocolVersion.VERSION_ONE).setType(
                                     OperationType.SUBSCRIBE).setTxnId(0).setTopic(topic).setSubscribeRequest(subRequestPrototype).build();

        ush = new UnsubscribeHandler(conf, tm, sm, dm, subChannelMgr);
    }

    @Test(timeout=60000)
    public void testNoSubscribeRequest() {
        sh.handleRequestAtOwner(PubSubRequest.newBuilder(pubSubRequestPrototype).clearSubscribeRequest().build(),
                                channel);
        assertEquals(StatusCode.MALFORMED_REQUEST, ((PubSubResponse) channel.getMessagesWritten().get(0))
                     .getStatusCode());
    }

    @Test(timeout=60000)
    public void testSuccessCase() {
        StubCallback<Void> callback = new StubCallback<Void>();
        sm.acquiredTopic(topic, callback, null);
        assertNull(ConcurrencyUtils.take(callback.queue).right());

        sh.handleRequestAtOwner(pubSubRequestPrototype, channel);
        assertEquals(StatusCode.SUCCESS, ((PubSubResponse) channel.getMessagesWritten().get(0)).getStatusCode());

        // make sure the channel was put in the maps
        Set<TopicSubscriber> topicSubs = new HashSet<TopicSubscriber>();
        topicSubs.add(new TopicSubscriber(topic, subscriberId));
        assertEquals(topicSubs,
                     subChannelMgr.channel2sub.get(channel));
        assertEquals(channel,
                     subChannelMgr.sub2Channel.get(new TopicSubscriber(topic, subscriberId)));

        // make sure delivery was started
        StartServingRequest startRequest = (StartServingRequest) dm.lastRequest.poll();
        assertEquals(channel, ((ChannelEndPoint) startRequest.endPoint).getChannel());
        assertEquals(PipelineFilter.class, startRequest.filter.getClass());
        PipelineFilter pfilter = (PipelineFilter)(startRequest.filter);
        assertEquals(1, pfilter.size());
        assertEquals(AllToAllTopologyFilter.class, pfilter.getFirst().getClass());
        assertEquals(1, startRequest.seqIdToStartFrom.getLocalComponent());
        assertEquals(subscriberId, startRequest.subscriberId);
        assertEquals(topic, startRequest.topic);

        // make sure subscription was registered
        StubCallback<SubscriptionData> callback1 = new StubCallback<SubscriptionData>();
        sm.serveSubscribeRequest(topic, SubscribeRequest.newBuilder(subRequestPrototype).setCreateOrAttach(
                                     CreateOrAttach.CREATE).build(), MessageSeqId.newBuilder().setLocalComponent(10).build(), callback1,
                                 null);

        assertEquals(PubSubException.ClientAlreadySubscribedException.class, ConcurrencyUtils.take(callback1.queue)
                     .right().getClass());

        // trying to subscribe again should throw an error
        WriteRecordingChannel dupChannel = new WriteRecordingChannel();
        sh.handleRequestAtOwner(pubSubRequestPrototype, dupChannel);
        assertEquals(StatusCode.TOPIC_BUSY, ((PubSubResponse) dupChannel.getMessagesWritten().get(0)).getStatusCode());

        // after disconnecting the channel, subscribe should work again
        subChannelMgr.channelDisconnected(channel);

        dupChannel = new WriteRecordingChannel();
        sh.handleRequestAtOwner(pubSubRequestPrototype, dupChannel);
        assertEquals(StatusCode.SUCCESS, ((PubSubResponse) dupChannel.getMessagesWritten().get(0)).getStatusCode());

        // test unsubscribe
        channel = new WriteRecordingChannel();
        ush.handleRequestAtOwner(pubSubRequestPrototype, channel);
        assertEquals(StatusCode.MALFORMED_REQUEST, ((PubSubResponse) channel.getMessagesWritten().get(0))
                     .getStatusCode());

        PubSubRequest unsubRequest = PubSubRequest.newBuilder(pubSubRequestPrototype).setUnsubscribeRequest(
                                         UnsubscribeRequest.newBuilder().setSubscriberId(subscriberId)).build();
        channel = new WriteRecordingChannel();
        dm.lastRequest.clear();

        ush.handleRequestAtOwner(unsubRequest, channel);
        assertEquals(StatusCode.SUCCESS, ((PubSubResponse) channel.getMessagesWritten().get(0)).getStatusCode());

        // make sure delivery has been stopped
        assertEquals(new TopicSubscriber(topic, subscriberId), dm.lastRequest.poll());

        // make sure the info is gone from the sm
        StubCallback<SubscriptionData> callback2 = new StubCallback<SubscriptionData>();
        sm.serveSubscribeRequest(topic, SubscribeRequest.newBuilder(subRequestPrototype).setCreateOrAttach(
                                     CreateOrAttach.ATTACH).build(), MessageSeqId.newBuilder().setLocalComponent(10).build(), callback2,
                                 null);
        assertEquals(PubSubException.ClientNotSubscribedException.class, ConcurrencyUtils.take(callback2.queue).right()
                     .getClass());

    }

}
