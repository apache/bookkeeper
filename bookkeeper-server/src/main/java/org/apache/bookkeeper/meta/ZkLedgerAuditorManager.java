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
package org.apache.bookkeeper.meta;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.proto.DataFormats.AuditorVoteFormat;
import static org.apache.bookkeeper.replication.ReplicationStats.ELECTION_ATTEMPTS;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * ZK based implementation of LedgerAuditorManager.
 */
@Slf4j
public class ZkLedgerAuditorManager implements LedgerAuditorManager {

    private final ZooKeeper zkc;
    private final ServerConfiguration conf;
    private final String basePath;
    private final String electionPath;

    private String myVote;

    private static final String ELECTION_ZNODE = "auditorelection";

    // Represents the index of the auditor node
    private static final int AUDITOR_INDEX = 0;
    // Represents vote prefix
    private static final String VOTE_PREFIX = "V_";
    // Represents path Separator
    private static final String PATH_SEPARATOR = "/";

    private volatile Consumer<AuditorEvent> listener;
    private volatile boolean isClosed = false;

    // Expose Stats
    @StatsDoc(
            name = ELECTION_ATTEMPTS,
            help = "The number of auditor election attempts"
    )
    private final Counter electionAttempts;

    public ZkLedgerAuditorManager(ZooKeeper zkc, ServerConfiguration conf, StatsLogger statsLogger) {
        this.zkc = zkc;
        this.conf = conf;

        this.basePath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        this.electionPath = basePath + '/' + ELECTION_ZNODE;
        this.electionAttempts = statsLogger.getCounter(ELECTION_ATTEMPTS);
    }

    @Override
    public void tryToBecomeAuditor(String bookieId, Consumer<AuditorEvent> listener)
            throws IOException, InterruptedException {
        this.listener = listener;
        createElectorPath();

        try {
            while (!isClosed) {
                createMyVote(bookieId);

                List<String> children = zkc.getChildren(getVotePath(""), false);
                if (0 >= children.size()) {
                    throw new IllegalArgumentException(
                            "At least one bookie server should present to elect the Auditor!");
                }

                // sorting in ascending order of sequential number
                Collections.sort(children, new ElectionComparator());
                String voteNode = StringUtils.substringAfterLast(myVote, PATH_SEPARATOR);

                if (children.get(AUDITOR_INDEX).equals(voteNode)) {
                    // We have been elected as the auditor
                    // update the auditor bookie id in the election path. This is
                    // done for debugging purpose
                    AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder()
                            .setBookieId(bookieId);

                    zkc.setData(getVotePath(""),
                            builder.build().toString().getBytes(UTF_8), -1);
                    return;
                 } else {
                    // If not an auditor, will be watching to my predecessor and
                    // looking the previous node deletion.
                    int myIndex = children.indexOf(voteNode);
                    if (myIndex < 0) {
                        throw new IllegalArgumentException("My vote has disappeared");
                    }

                    int prevNodeIndex = myIndex - 1;

                    CountDownLatch latch = new CountDownLatch(1);

                    if (null == zkc.exists(getVotePath(PATH_SEPARATOR)
                            + children.get(prevNodeIndex), event -> latch.countDown())) {
                        // While adding, the previous znode doesn't exists.
                        // Again going to election.
                        continue;
                    }

                    // Wait for the previous auditor in line to be deleted
                    latch.await();
                }

                electionAttempts.inc();
            }
        } catch (KeeperException e) {
            throw new IOException(e);
        }
    }

    @Override
    public BookieId getCurrentAuditor() throws IOException, InterruptedException {
        String electionRoot = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE + '/' + ELECTION_ZNODE;

        try {
            List<String> children = zkc.getChildren(electionRoot, false);
            Collections.sort(children, new ElectionComparator());
            if (children.size() < 1) {
                return null;
            }
            String ledger = electionRoot + "/" + children.get(AUDITOR_INDEX);
            byte[] data = zkc.getData(ledger, false, null);

            AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder();
            TextFormat.merge(new String(data, UTF_8), builder);
            AuditorVoteFormat v = builder.build();
            return BookieId.parse(v.getBookieId());
        } catch (KeeperException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        log.info("Shutting down AuditorElector");
        isClosed = true;
        if (myVote != null) {
            try {
                zkc.delete(myVote, -1);
            } catch (KeeperException.NoNodeException nne) {
                // Ok
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("InterruptedException while deleting myVote: " + myVote,
                        ie);
            } catch (KeeperException ke) {
                log.error("Exception while deleting myVote:" + myVote, ke);
            }
        }
    }

    private void createMyVote(String bookieId) throws IOException, InterruptedException {
        List<ACL> zkAcls = ZkUtils.getACLs(conf);
        AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder()
                .setBookieId(bookieId);

        try {
            if (null == myVote || null == zkc.exists(myVote, false)) {
                myVote = zkc.create(getVotePath(PATH_SEPARATOR + VOTE_PREFIX),
                        builder.build().toString().getBytes(UTF_8), zkAcls,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
            }
        } catch (KeeperException e) {
            throw new IOException(e);
        }
    }

    private void createElectorPath() throws IOException {
        try {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            if (zkc.exists(basePath, false) == null) {
                try {
                    zkc.create(basePath, new byte[0], zkAcls,
                            CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    // do nothing, someone else could have created it
                }
            }
            if (zkc.exists(getVotePath(""), false) == null) {
                try {
                    zkc.create(getVotePath(""), new byte[0],
                            zkAcls, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException nee) {
                    // do nothing, someone else could have created it
                }
            }
        } catch (KeeperException ke) {
            throw new IOException("Failed to initialize Auditor Elector", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Failed to initialize Auditor Elector", ie);
        }
    }

    private String getVotePath(String vote) {
        return electionPath + vote;
    }

    private void handleZkWatch(WatchedEvent event) {
        if (isClosed) {
            return;
        }

        if (event.getState() == Watcher.Event.KeeperState.Expired) {
            log.error("Lost ZK connection, shutting down");

            listener.accept(AuditorEvent.SessionLost);
        } else if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
            listener.accept(AuditorEvent.VoteWasDeleted);
        }
    }

    /**
     * Compare the votes in the ascending order of the sequence number. Vote
     * format is 'V_sequencenumber', comparator will do sorting based on the
     * numeric sequence value.
     */
    private static class ElectionComparator
            implements Comparator<String>, Serializable {
        /**
         * Return -1 if the first vote is less than second. Return 1 if the
         * first vote is greater than second. Return 0 if the votes are equal.
         */
        @Override
        public int compare(String vote1, String vote2) {
            long voteSeqId1 = getVoteSequenceId(vote1);
            long voteSeqId2 = getVoteSequenceId(vote2);
            int result = voteSeqId1 < voteSeqId2 ? -1
                    : (voteSeqId1 > voteSeqId2 ? 1 : 0);
            return result;
        }

        private long getVoteSequenceId(String vote) {
            String voteId = StringUtils.substringAfter(vote, VOTE_PREFIX);
            return Long.parseLong(voteId);
        }
    }

}
