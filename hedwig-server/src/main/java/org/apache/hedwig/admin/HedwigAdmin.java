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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRange;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.meta.FactoryLayout;
import org.apache.hedwig.server.meta.SubscriptionDataManager;
import org.apache.hedwig.server.meta.TopicOwnershipManager;
import org.apache.hedwig.server.meta.TopicPersistenceManager;
import org.apache.hedwig.server.subscriptions.InMemorySubscriptionState;
import org.apache.hedwig.server.topics.HubInfo;
import org.apache.hedwig.server.topics.HubLoad;
import org.apache.hedwig.util.Callback;
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

    protected final ZooKeeper zk;
    protected final BookKeeper bk;
    protected final MetadataManagerFactory mmFactory;
    protected final SubscriptionDataManager sdm;
    protected final TopicOwnershipManager tom;
    protected final TopicPersistenceManager tpm;

    // hub configurations
    protected final ServerConfiguration serverConf;
    // bookkeeper configurations
    protected final ClientConfiguration bkClientConf;

    protected final CountDownLatch zkReadyLatch = new CountDownLatch(1);

    // Empty watcher
    private class MyWatcher implements Watcher {
        public void process(WatchedEvent event) {
            if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                zkReadyLatch.countDown();
            }
        }
    }

    static class SyncObj<T> {
        boolean finished = false;
        boolean success = false;
        T value = null;
        PubSubException exception = null;

        synchronized void success(T v) {
            finished = true;
            success = true;
            value = v;
            notify();
        }

        synchronized void fail(PubSubException pse) {
            finished = true;
            success = false;
            exception = pse;
            notify();
        }

        synchronized void block() {
            try {
                while (!finished) {
                    wait();
                }
            } catch (InterruptedException ie) {
            }
        }

        synchronized boolean isSuccess() {
            return success;
        }
    }

    /**
     * Stats of a hub
     */
    public static class HubStats {
        HubInfo hubInfo;
        HubLoad hubLoad;

        public HubStats(HubInfo info, HubLoad load) {
            this.hubInfo = info;
            this.hubLoad = load;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("info : [").append(hubInfo.toString().trim().replaceAll("\n", ", "))
              .append("], load : [").append(hubLoad.toString().trim().replaceAll("\n", ", "))
              .append("]");
            return sb.toString();
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
        LOG.debug("Connecting to zookeeper {}, timeout = {}",
                hubConf.getZkHost(), hubConf.getZkTimeout());
        // wait until connection is ready
        if (!zkReadyLatch.await(hubConf.getZkTimeout() * 2, TimeUnit.MILLISECONDS)) {
            throw new Exception("Count not establish connection with ZooKeeper after " + hubConf.getZkTimeout() * 2 + " ms.");
        }

        // construct the metadata manager factory
        mmFactory = MetadataManagerFactory.newMetadataManagerFactory(hubConf, zk);
        tpm = mmFactory.newTopicPersistenceManager();
        tom = mmFactory.newTopicOwnershipManager();
        sdm = mmFactory.newSubscriptionDataManager();

        // connect to bookkeeper
        bk = new BookKeeper(bkClientConf, zk);
        LOG.debug("Connecting to bookkeeper");
    }

    /**
     * Close the hedwig admin.
     *
     * @throws Exception
     */
    public void close() throws Exception {
        tpm.close();
        tom.close();
        sdm.close();
        mmFactory.shutdown();
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
     * Return metadata manager factory.
     *
     * @return metadata manager factory instance.
     */
    public MetadataManagerFactory getMetadataManagerFactory() {
        return mmFactory;
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
        // current persistence info is bound with a topic, so if there is persistence info
        // there is topic.
        final SyncObj<Boolean> syncObj = new SyncObj<Boolean>();
        tpm.readTopicPersistenceInfo(topic, new Callback<Versioned<LedgerRanges>>() {
            @Override
            public void operationFinished(Object ctx, Versioned<LedgerRanges> result) {
                if (null == result) {
                    syncObj.success(false);
                } else {
                    syncObj.success(true);
                }
            }
            @Override
            public void operationFailed(Object ctx, PubSubException pse) {
                syncObj.fail(pse);
            }
        }, syncObj);

        syncObj.block();

        if (!syncObj.isSuccess()) {
            throw syncObj.exception;
        }

        return syncObj.value;
    }

    /**
     * Get available hubs.
     *
     * @return available hubs and their loads
     * @throws Exception
     */
    public Map<HedwigSocketAddress, HubStats> getAvailableHubs() throws Exception {
        String zkHubsPath = serverConf.getZkHostsPrefix(new StringBuilder()).toString();
        Map<HedwigSocketAddress, HubStats> hubs =
            new HashMap<HedwigSocketAddress, HubStats>();
        List<String> hosts = zk.getChildren(zkHubsPath, false);
        for (String host : hosts) {
            String zkHubPath = serverConf.getZkHostsPrefix(new StringBuilder())
                                         .append("/").append(host).toString();
            HedwigSocketAddress addr = new HedwigSocketAddress(host);
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData(zkHubPath, false, stat);
                if (data == null) {
                    continue;
                }
                HubLoad load = HubLoad.parse(new String(data));
                HubInfo info = new HubInfo(addr, stat.getCzxid());
                hubs.put(addr, new HubStats(info, load));
            } catch (KeeperException ke) {
                LOG.warn("Couldn't read hub data from ZooKeeper", ke);
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted during read", ie);
            }
        }
        return hubs;
    }

    /**
     * Get list of topics
     *
     * @return list of topics
     * @throws Exception
     */
    public Iterator<ByteString> getTopics() throws Exception {
        return mmFactory.getTopics();
    }

    /**
     * Return the topic owner of a topic
     *
     * @param topic
     *            Topic name
     * @return the address of the owner of a topic
     * @throws Exception
     */
    public HubInfo getTopicOwner(ByteString topic) throws Exception {
        final SyncObj<HubInfo> syncObj = new SyncObj<HubInfo>();
        tom.readOwnerInfo(topic, new Callback<Versioned<HubInfo>>() {
            @Override
            public void operationFinished(Object ctx, Versioned<HubInfo> result) {
                if (null == result) {
                    syncObj.success(null);
                } else {
                    syncObj.success(result.getValue());
                }
            }
            @Override
            public void operationFailed(Object ctx, PubSubException pse) {
                syncObj.fail(pse);
            }
        }, syncObj);

        syncObj.block();

        if (!syncObj.isSuccess()) {
            throw syncObj.exception;
        }

        return syncObj.value;
    }

    private static LedgerRange buildLedgerRange(long ledgerId, long startOfLedger, MessageSeqId endOfLedger) {
        LedgerRange.Builder builder =
            LedgerRange.newBuilder().setLedgerId(ledgerId).setStartSeqIdIncluded(startOfLedger)
                       .setEndSeqIdIncluded(endOfLedger);
        return builder.build();
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
        final SyncObj<LedgerRanges> syncObj = new SyncObj<LedgerRanges>();
        tpm.readTopicPersistenceInfo(topic, new Callback<Versioned<LedgerRanges>>() {
            @Override
            public void operationFinished(Object ctx, Versioned<LedgerRanges> result) {
                if (null == result) {
                    syncObj.success(null);
                } else {
                    syncObj.success(result.getValue());
                }
            }
            @Override
            public void operationFailed(Object ctx, PubSubException pse) {
                syncObj.fail(pse);
            }
        }, syncObj);

        syncObj.block();

        if (!syncObj.isSuccess()) {
            throw syncObj.exception;
        }

        LedgerRanges ranges = syncObj.value;
        if (null == ranges) {
            return null;
        }
        List<LedgerRange> results = new ArrayList<LedgerRange>();
        List<LedgerRange> lrs = ranges.getRangesList();
        long startSeqId = 1L;
        if (!lrs.isEmpty()) {
            LedgerRange range = lrs.get(0);
            if (!range.hasStartSeqIdIncluded() && range.hasEndSeqIdIncluded()) {
                long ledgerId = range.getLedgerId();
                try {
                    LedgerHandle lh = bk.openLedgerNoRecovery(ledgerId, DigestType.CRC32, passwd);
                    long numEntries = lh.readLastConfirmed() + 1;
                    long endOfLedger = range.getEndSeqIdIncluded().getLocalComponent();
                    startSeqId = endOfLedger - numEntries + 1;
                } catch (BKException.BKNoSuchLedgerExistsException be) {
                    // ignore it
                }
            }
        }
        Iterator<LedgerRange> lrIter = lrs.iterator();
        while (lrIter.hasNext()) {
            LedgerRange range = lrIter.next();
            if (range.hasEndSeqIdIncluded()) {
                long endOfLedger = range.getEndSeqIdIncluded().getLocalComponent();
                if (range.hasStartSeqIdIncluded()) {
                    startSeqId = range.getStartSeqIdIncluded();
                } else {
                    range = buildLedgerRange(range.getLedgerId(), startSeqId, range.getEndSeqIdIncluded());
                }
                results.add(range);
                if (startSeqId < endOfLedger + 1) {
                    startSeqId = endOfLedger + 1;
                }
                continue;
            }
            if (lrIter.hasNext()) {
                throw new IllegalStateException("Ledger " + range.getLedgerId() + " for topic " + topic.toString()
                                                + " is not the last one but still does not have an end seq-id");
            }

            if (range.hasStartSeqIdIncluded()) {
                startSeqId = range.getStartSeqIdIncluded();
            }

            LedgerHandle lh = bk.openLedgerNoRecovery(range.getLedgerId(), DigestType.CRC32, passwd);
            long endOfLedger = startSeqId + lh.readLastConfirmed();
            MessageSeqId endSeqId = MessageSeqId.newBuilder().setLocalComponent(endOfLedger).build();
            results.add(buildLedgerRange(range.getLedgerId(), startSeqId, endSeqId));
        }
        return results;
    }

    /**
     * Return subscriptions of a topic
     *
     * @param topic
     *          Topic name
     * @return subscriptions of a topic
     * @throws Exception
     */
    public Map<ByteString, SubscriptionData> getTopicSubscriptions(ByteString topic)
        throws Exception {

        final SyncObj<Map<ByteString, SubscriptionData>> syncObj =
            new SyncObj<Map<ByteString, SubscriptionData>>();
        sdm.readSubscriptions(topic, new Callback<Map<ByteString, Versioned<SubscriptionData>>>() {
            @Override
            public void operationFinished(Object ctx, Map<ByteString, Versioned<SubscriptionData>> result) {
                // It was just used to console tool to print some information, so don't need to return version for it
                // just keep the getTopicSubscriptions interface as before
                Map<ByteString, SubscriptionData> subs = new ConcurrentHashMap<ByteString, SubscriptionData>();
                for (Map.Entry<ByteString, Versioned<SubscriptionData>> subEntry : result.entrySet()) {
                    subs.put(subEntry.getKey(), subEntry.getValue().getValue());
                }
                syncObj.success(subs);
            }
            @Override
            public void operationFailed(Object ctx, PubSubException pse) {
                syncObj.fail(pse);
            }
        }, syncObj);

        syncObj.block();

        if (!syncObj.isSuccess()) {
            throw syncObj.exception;
        }

        return syncObj.value;
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
    public SubscriptionData getSubscription(ByteString topic, ByteString subscriber) throws Exception {
        final SyncObj<SubscriptionData> syncObj = new SyncObj<SubscriptionData>();
        sdm.readSubscriptionData(topic, subscriber, new Callback<Versioned<SubscriptionData>>() {
            @Override
            public void operationFinished(Object ctx, Versioned<SubscriptionData> result) {
                if (null == result) {
                    syncObj.success(null);
                } else {
                    syncObj.success(result.getValue());
                }
            }
            @Override
            public void operationFailed(Object ctx, PubSubException pse) {
                syncObj.fail(pse);
            }
        }, syncObj);

        syncObj.block();

        if (!syncObj.isSuccess()) {
            throw syncObj.exception;
        }

        return syncObj.value;
    }

    /**
     * Format metadata for Hedwig.
     */
    public void format() throws Exception {
        // format metadata first
        mmFactory.format(serverConf, zk);
        LOG.info("Formatted Hedwig metadata successfully.");
        // remove metadata layout
        FactoryLayout.deleteLayout(zk, serverConf);
        LOG.info("Removed old factory layout.");
        // create new metadata manager factory and write new metadata layout
        MetadataManagerFactory.createMetadataManagerFactory(serverConf, zk,
            serverConf.getMetadataManagerFactoryClass());
        LOG.info("Created new factory layout.");
    }
}
