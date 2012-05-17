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

package org.apache.hedwig.admin;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRange;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Hedwig Admin
 */
public class HedwigAdmin {
    static final Logger LOG = LoggerFactory.getLogger(HedwigAdmin.class);

    // NOTE: now it is fixed passwd used in hedwig
    static byte[] passwd = "sillysecret".getBytes();

    protected ZooKeeper zk;
    protected BookKeeper bk;
    // hub configurations
    protected ServerConfiguration serverConf;
    // bookkeeper configurations
    protected ClientConfiguration bkClientConf;

    // Empty watcher
    private static class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
        }
    }

    /**
     * Hedwig Admin Constructor
     *
     * @param bkConf
     *          BookKeeper Client Configuration.
     * @param hubConf
     *          Hub Server Configuration.
     * @throws Exception
     */
    public HedwigAdmin(ClientConfiguration bkConf, ServerConfiguration hubConf) throws Exception {
        this.serverConf = hubConf;
        this.bkClientConf = bkConf;

        // connect to zookeeper
        zk = new ZooKeeper(hubConf.getZkHost(), hubConf.getZkTimeout(), new MyWatcher());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to zookeeper " + hubConf.getZkHost() + ", timeout = "
                    + hubConf.getZkTimeout());
        }

        // connect to bookkeeper
        bk = new BookKeeper(bkClientConf, zk);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to bookkeeper");
        }
    }

    /**
     * Close the hedwig admin.
     *
     * @throws Exception
     */
    public void close() throws Exception {
        bk.close();
        zk.close();
    }

    /**
     * Return zookeeper handle used in hedwig admin.
     *
     * @return zookeeper handle
     */
    public ZooKeeper getZkHandle() {
        return zk;
    }

    /**
     * Return bookkeeper handle used in hedwig admin.
     *
     * @return bookkeeper handle
     */
    public BookKeeper getBkHandle() {
        return bk;
    }

    /**
     * Return hub server configuration used in hedwig admin
     *
     * @return hub server configuration
     */
    public ServerConfiguration getHubServerConf() {
        return serverConf;
    }

    /**
     * Return bookeeper passwd used in hedwig admin
     *
     * @return bookeeper passwd
     */
    public byte[] getBkPasswd() {
        return Arrays.copyOf(passwd, passwd.length);
    }

    /**
     * Return digest type used in hedwig admin
     *
     * @return bookeeper digest type
     */
    public DigestType getBkDigestType() {
        return DigestType.CRC32;
    }

    /**
     * Dose topic exist?
     *
     * @param topic
     *            Topic name
     * @return whether topic exists or not?
     * @throws Exception
     */
    public boolean hasTopic(ByteString topic) throws Exception {
        String topicPath = serverConf.getZkTopicPath(new StringBuilder(), topic).toString();
        Stat stat = zk.exists(topicPath, false);
        return null != stat;
    }

    /**
     * Get available hubs.
     *
     * @return available hubs and their loads
     * @throws Exception
     */
    public Map<HedwigSocketAddress, Integer> getAvailableHubs() throws Exception {
        String zkHubsPath = serverConf.getZkHostsPrefix(new StringBuilder()).toString();
        Map<HedwigSocketAddress, Integer> hubs =
            new HashMap<HedwigSocketAddress, Integer>();
        List<String> hosts = zk.getChildren(zkHubsPath, false);
        for (String host : hosts) {
            String zkHubPath = serverConf.getZkHostsPrefix(new StringBuilder())
                                         .append("/").append(host).toString();
            int load = 0;
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData(zkHubPath, false, stat);
                if (data != null) {
                    load = Integer.parseInt(new String(data));
                }
            } catch (KeeperException ke) {
                LOG.warn("Couldn't read hub data from ZooKeeper", ke);
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted during read", ie);
            }
            hubs.put(new HedwigSocketAddress(host), load);
        }
        return hubs;
    }

    /**
     * Get list of topics
     *
     * @return list of topics
     * @throws Exception
     */
    public List<String> getTopics() throws Exception {
        return zk.getChildren(serverConf.getZkTopicsPrefix(new StringBuilder()).toString(), false);
    }

    /**
     * Return the znode path of owner of a topic
     *
     * @param topic
     *            Topic name
     * @return znode path of owner of a topic
     */
    String hubPath(ByteString topic) {
        return serverConf.getZkTopicPath(new StringBuilder(), topic).append("/hub").toString();
    }

    /**
     * Return the topic owner of a topic
     *
     * @param topic
     *            Topic name
     * @return the address of the owner of a topic
     * @throws Exception
     */
    public HedwigSocketAddress getTopicOwner(ByteString topic) throws Exception {
        Stat stat = new Stat();
        byte[] owner = null;
        try {
            owner = zk.getData(hubPath(topic), false, stat);
        } catch (KeeperException.NoNodeException nne) {
        }
        if (null == owner) {
            return null;
        }
        return new HedwigSocketAddress(new String(owner));
    }

    /**
     * Return the znode path to store ledgers info of a topic
     *
     * @param topic
     *          Topic name
     * @return znode path to store ledgers info of a topic
     */
    String ledgersPath(ByteString topic) {
        return serverConf.getZkTopicPath(new StringBuilder(), topic).append("/ledgers").toString();
    }

    /**
     * Return the ledger range forming the topic
     *
     * @param topic
     *          Topic name
     * @return ledger ranges forming the topic
     * @throws Exception
     */
    public List<LedgerRange> getTopicLedgers(ByteString topic) throws Exception {
        LedgerRanges ranges = null;
        try {
            Stat stat = new Stat();
            byte[] ledgersData = zk.getData(ledgersPath(topic), false, stat);
            if (null != ledgersData) {
                ranges = LedgerRanges.parseFrom(ledgersData);
            }
        } catch (KeeperException.NoNodeException nne) {
        }
        if (null == ranges) {
            return null;
        }
        List<LedgerRange> lrs = ranges.getRangesList();
        if (lrs.isEmpty()) {
            return lrs;
        }
        // try to check last ledger (it may still open)
        LedgerRange lastRange = lrs.get(lrs.size() - 1);
        if (lastRange.hasEndSeqIdIncluded()) {
            return lrs;
        }
        // read last confirmed of the opened ledger
        try {
            List<LedgerRange> newLrs = new ArrayList<LedgerRange>();
            newLrs.addAll(lrs);
            lrs = newLrs;
            MessageSeqId lastSeqId;
            if (lrs.size() == 1) {
                lastSeqId = MessageSeqId.newBuilder().setLocalComponent(1).build();
            } else {
                lastSeqId = lrs.get(lrs.size() - 2).getEndSeqIdIncluded();
            }
            LedgerRange newLastRange = refreshLastLedgerRange(lastSeqId, lastRange);
            lrs.set(lrs.size() - 1, newLastRange);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lrs;
    }

    /**
     * Refresh last ledger range to get lastConfirmed entry, which make it available to read
     *
     * @param lastSeqId
     *            Last sequence id of previous ledger
     * @param oldRange
     *            Ledger range to set lastConfirmed entry
     */
    LedgerRange refreshLastLedgerRange(MessageSeqId lastSeqId, LedgerRange oldRange)
        throws BKException, KeeperException, InterruptedException {
        LedgerHandle lh = bk.openLedgerNoRecovery(oldRange.getLedgerId(), DigestType.CRC32, passwd);
        long lastConfirmed = lh.readLastConfirmed();
        MessageSeqId newSeqId = MessageSeqId.newBuilder().mergeFrom(lastSeqId)
                                .setLocalComponent(lastSeqId.getLocalComponent() + lastConfirmed).build();
        return LedgerRange.newBuilder().mergeFrom(oldRange).setEndSeqIdIncluded(newSeqId).build();
    }

    /**
     * Return the znode path store all the subscribers of a topic.
     *
     * @param sb
     *          String builder to hold the znode path
     * @param topic
     *          Topic name
     */
    private StringBuilder topicSubscribersPath(StringBuilder sb, ByteString topic) {
        return serverConf.getZkTopicPath(sb, topic).append("/subscribers");
    }

    /**
     * Return the znode path of a subscriber of a topic.
     *
     * @param topic
     *          Topic name
     * @param subscriber
     *          Subscriber name
     */

    private String topicSubscriberPath(ByteString topic, ByteString subscriber) {
        return topicSubscribersPath(new StringBuilder(), topic).append("/")
               .append(subscriber.toStringUtf8()).toString();
    }

    /**
     * Return subscriptions of a topic
     *
     * @param topic
     *          Topic name
     * @return subscriptions of a topic
     * @throws Exception
     */
    public Map<ByteString, SubscriptionState> getTopicSubscriptions(ByteString topic)
        throws Exception {
        Map<ByteString, SubscriptionState> states =
            new HashMap<ByteString, SubscriptionState>();
        try {
            String subsPath = topicSubscribersPath(new StringBuilder(), topic).toString();
            List<String> children = zk.getChildren(subsPath, false);
            for (String child : children) {
                ByteString subscriberId = ByteString.copyFromUtf8(child);
                String subPath = topicSubscriberPath(topic, subscriberId);
                Stat stat = new Stat();
                byte[] subData = zk.getData(subPath, false, stat);
                if (null == subData) {
                    continue;
                }
                SubscriptionState state = SubscriptionState.parseFrom(subData);
                states.put(subscriberId, state);
            }
        } catch (KeeperException.NoNodeException nne) {
        }
        return states;
    }

    /**
     * Return subscription state of a subscriber of topic
     *
     * @param topic
     *          Topic name
     * @param subscriber
     *          Subscriber name
     * @return subscription state
     * @throws Exception
     */
    public SubscriptionState getSubscription(ByteString topic, ByteString subscriber) throws Exception {
        String subPath = topicSubscriberPath(topic, subscriber);
        Stat stat = new Stat();
        byte[] subData = null;
        try {
            subData = zk.getData(subPath, false, stat);
        } catch (KeeperException.NoNodeException nne) {
        }
        if (null == subData) {
            return null;
        }
        return SubscriptionState.parseFrom(subData);
    }
}
