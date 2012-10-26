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

package org.apache.hedwig.admin.console;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.hedwig.admin.HedwigAdmin;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRange;
import org.apache.hedwig.protocol.PubSubProtocol.LedgerRanges;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.RegionSpecificSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A tool to read topic messages.
 *
 * This tool :
 * 1) read persistence info from zookeeper: ledger ranges
 * 2) read subscription infor from zookeeper: we can know the least message id (ledger id) 
 * 3) use bk client to read message starting from least message id
 */
public class ReadTopic {
    
    final HedwigAdmin admin;
    final ByteString topic;
    long startSeqId;
    long leastConsumedSeqId = Long.MAX_VALUE;
    final boolean inConsole;

    static final int RC_OK = 0;
    static final int RC_ERROR = -1;
    static final int RC_NOTOPIC = -2;
    static final int RC_NOLEDGERS = -3;
    static final int RC_NOSUBSCRIBERS = -4;
    
    static final int NUM_MESSAGES_TO_PRINT = 15;

    List<LedgerRange> ledgers = new ArrayList<LedgerRange>();
    
    /**
     * Constructor
     */
    public ReadTopic(HedwigAdmin admin, ByteString topic, boolean inConsole) {
        this(admin, topic, 1, inConsole);
    }

    /**
     * Constructor
     */
    public ReadTopic(HedwigAdmin admin, ByteString topic, long msgSeqId, boolean inConsole) {
        this.admin = admin;
        this.topic = topic;
        this.startSeqId = msgSeqId;
        this.inConsole = inConsole;
    }
    
    /**
     * Check whether the topic existed or not
     *
     * @return RC_OK if topic is existed; RC_NOTOPIC if not.
     * @throws Exception
     */
    protected int checkTopic() throws Exception {
        return admin.hasTopic(topic) ? RC_OK : RC_NOTOPIC;
    }
    
    /**
     * Get the ledgers used by this topic to store messages
     *
     * @return RC_OK if topic has messages; RC_NOLEDGERS if not.
     * @throws Exception
     */
    protected int getTopicLedgers() throws Exception {
        List<LedgerRange> ranges = admin.getTopicLedgers(topic); 
        if (null == ranges || ranges.isEmpty()) {
            return RC_NOLEDGERS;
        }
        ledgers.addAll(ranges);
        return RC_OK;
    }
    
    protected int getLeastSubscription() throws Exception {
        Map<ByteString, SubscriptionData> states = admin.getTopicSubscriptions(topic); 
        if (states.isEmpty()) {
            return RC_NOSUBSCRIBERS;
        }
        for (Map.Entry<ByteString, SubscriptionData> entry : states.entrySet()) {
            SubscriptionData state = entry.getValue();
            long localMsgId = state.getState().getMsgId().getLocalComponent();
            if (localMsgId < leastConsumedSeqId) {
                leastConsumedSeqId = localMsgId;
            }
        }
        if (leastConsumedSeqId == Long.MAX_VALUE) {
            leastConsumedSeqId = 0;
        }
        return RC_OK;
    }
    
    public void readTopic() {
        try {
            int rc = _readTopic();
            switch (rc) {
            case RC_NOTOPIC:
                System.err.println("No topic " + topic + " found.");
                break;
            case RC_NOLEDGERS:
                System.err.println("No message is published to topic " + topic);
                break;
            default:
                break;
            }
        } catch (Exception e) {
            System.err.println("ERROR: read messages of topic " + topic + " failed.");
            e.printStackTrace();
        }
    }
    
    protected int _readTopic() throws Exception {
        int rc;
        // check topic
        rc = checkTopic();
        if (RC_OK != rc) {
            return rc;
        }
        // get topic ledgers
        rc = getTopicLedgers();
        if (RC_OK != rc) {
            return rc;
        }
        // get topic subscription to find the least one
        rc = getLeastSubscription();
        if (RC_NOSUBSCRIBERS == rc) {
            startSeqId = 1;
        } else if (RC_OK == rc) {
            if (leastConsumedSeqId > startSeqId) {
                startSeqId = leastConsumedSeqId + 1;
            }
        } else {
            return rc;
        }

        for (LedgerRange range : ledgers) {
            long endSeqId = range.getEndSeqIdIncluded().getLocalComponent();
            if (endSeqId < startSeqId) {
                continue;
            }
            boolean toContinue = readLedger(range);
            startSeqId = endSeqId + 1;
            if (!toContinue) {
                break;
            }
        }
        
        return RC_OK;
    }
    
    /**
     * Read a specific ledger
     *
     * @param ledger in memory ledger range
     * @param endSeqId end seq id
     * @return true if continue, otherwise false
     * @throws BKException
     * @throws IOException
     * @throws InterruptedException
     */
    protected boolean readLedger(LedgerRange ledger)
    throws BKException, IOException, InterruptedException {
        long tEndSeqId = ledger.getEndSeqIdIncluded().getLocalComponent();

        if (tEndSeqId < this.startSeqId) {
            return true;
        }
        // Open Ledger Handle
        long ledgerId = ledger.getLedgerId();
        System.out.println("\n>>>>> " + ledger + " <<<<<\n");
        LedgerHandle lh = null;
        try {
            lh = admin.getBkHandle().openLedgerNoRecovery(ledgerId, admin.getBkDigestType(), admin.getBkPasswd());
        } catch (BKException e) {
            System.err.println("ERROR: No ledger " + ledgerId + " found. maybe garbage collected due to the messages are consumed.");
        }
        if (null == lh) {
            return true;
        }
        long expectedEntryId = startSeqId - ledger.getStartSeqIdIncluded();
        
        long correctedEndSeqId = tEndSeqId;
        try {
            while (startSeqId <= tEndSeqId) {
                correctedEndSeqId = Math.min(startSeqId + NUM_MESSAGES_TO_PRINT - 1, tEndSeqId);
                
                try {
                    Enumeration<LedgerEntry> seq =
                        lh.readEntries(startSeqId - ledger.getStartSeqIdIncluded(),
                                       correctedEndSeqId - ledger.getStartSeqIdIncluded());
                    LedgerEntry entry = null;
                    while (seq.hasMoreElements()) {
                        entry = seq.nextElement();
                        Message message;
                        try {
                            message = Message.parseFrom(entry.getEntryInputStream());
                        } catch (IOException e) {
                            System.out.println("WARN: Unreadable message found\n");
                            expectedEntryId++;
                            continue;
                        }
                        if (expectedEntryId != entry.getEntryId()
                            || (message.getMsgId().getLocalComponent() - ledger.getStartSeqIdIncluded()) != expectedEntryId) {
                            throw new IOException("ERROR: Message ids are out of order : expected entry id " + expectedEntryId
                                                + ", current entry id " + entry.getEntryId() + ", msg seq id " + message.getMsgId().getLocalComponent());
                        }
                        expectedEntryId++;
                        formatMessage(message);

                    }
                    startSeqId = correctedEndSeqId + 1;
                    if (inConsole) {
                        if (!pressKeyToContinue()) {
                            return false;
                        }
                    }
                } catch (BKException.BKReadException be) {
                    throw be;
                }
            }
        } catch (BKException bke) {
            if (tEndSeqId != Long.MAX_VALUE) {
                System.err.println("ERROR: ledger " + ledgerId + " may be corrupted, since read messages ["
                                 + startSeqId + " ~ " + correctedEndSeqId + " ] failed :");
                throw bke;
            }
        }
        System.out.println("\n");
        return true;
    }
    
    protected void formatMessage(Message message) {
        // print msg id
        String msgId;
        if (!message.hasMsgId()) {
            msgId = "N/A";
        } else {
            MessageSeqId seqId = message.getMsgId();
            StringBuilder idBuilder = new StringBuilder();
            if (seqId.hasLocalComponent()) {
                idBuilder.append("LOCAL(").append(seqId.getLocalComponent()).append(")");
            } else {
                List<RegionSpecificSeqId> remoteIds = seqId.getRemoteComponentsList();
                int i = 0, numRegions = remoteIds.size();
                idBuilder.append("REMOTE(");
                for (RegionSpecificSeqId rssid : remoteIds) {
                    idBuilder.append(rssid.getRegion().toStringUtf8());
                    idBuilder.append("[");
                    idBuilder.append(rssid.getSeqId());
                    idBuilder.append("]");
                    ++i;
                    if (i < numRegions) {
                        idBuilder.append(",");
                    }
                }
                idBuilder.append(")");
            }
            msgId = idBuilder.toString();
        }
        System.out.println("---------- MSGID=" + msgId + " ----------");
        System.out.println("MsgId:     " + msgId);
        // print source region
        if (message.hasSrcRegion()) {
            System.out.println("SrcRegion: " + message.getSrcRegion().toStringUtf8());
        } else {
            System.out.println("SrcRegion: N/A");
        }
        // print message body
        System.out.println("Message:");
        System.out.println();
        if (message.hasBody()) {
            System.out.println(message.getBody().toStringUtf8());
        } else {
            System.out.println("N/A");
        }
        System.out.println();
    }
    
    boolean pressKeyToContinue() throws IOException {
        System.out.println("Press Y to continue...");
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
        int ch = stdin.read();
        if (ch == 'y' ||
            ch == 'Y') {
            return true;
        }
        return false;
    }
}
