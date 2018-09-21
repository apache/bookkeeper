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

package org.apache.bookkeeper.meta.zk;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import com.google.protobuf.TextFormat.ParseException;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.AuditorSelector;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.AuditorVoteFormat;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 * ZooKeeper based {@link AuditorSelector} implementation.
 */
@Slf4j
class ZkAuditorSelector implements AuditorSelector {

    // Represents the index of the auditor node
    private static final int AUDITOR_INDEX = 0;
    // Represents vote prefix
    private static final String VOTE_PREFIX = "V_";
    // Represents path Separator
    private static final String PATH_SEPARATOR = "/";
    private static final String ELECTION_ZNODE = "auditorelection";
    // Represents urLedger path in zk
    private final String basePath;
    // Represents auditor election path in zk
    private final String electionPath;
    private final ZooKeeper zkc;
    private final ServerConfiguration conf;
    private final String bookieId;
    private String myVote;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService executor;

    ZkAuditorSelector(String bookieId,
                      ZooKeeper zkc,
                      ServerConfiguration conf) {
        this.bookieId = bookieId;
        this.zkc = zkc;
        this.conf = conf;
        basePath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        electionPath = basePath + '/' + ELECTION_ZNODE;
        executor = Executors.newSingleThreadExecutor(r ->
            new Thread(r, "AuditorElector"));
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void createMyVote()
            throws KeeperException, InterruptedException {
        if (null == myVote || null == zkc.exists(myVote, false)) {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder()
                .setBookieId(bookieId);
            myVote = zkc.create(getVotePath(PATH_SEPARATOR + VOTE_PREFIX),
                    TextFormat.printToString(builder.build()).getBytes(UTF_8), zkAcls,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        }
    }

    private String getVotePath(String vote) {
        return electionPath + vote;
    }

    private void createElectorPath() throws KeeperException, InterruptedException {
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
    }

    /**
     * Watching the predecessor bookies and will do election on predecessor node
     * deletion or expiration.
     */
    private class ElectionWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == KeeperState.Expired) {
                log.error("Lost ZK connection, shutting down");
                submitShutdownTask(null);
            } else if (event.getType() == EventType.NodeDeleted) {
                submitElectionTask(null);
            }
        }
    }

    @Override
    public BookieSocketAddress getCurrentAuditor() throws MetadataException {
        try {
            List<String> children = zkc.getChildren(electionPath, false);
            Collections.sort(children, new ElectionComparator());
            if (children.size() < 1) {
                return null;
            }
            String ledger = electionPath + "/" + children.get(AUDITOR_INDEX);
            byte[] data = zkc.getData(ledger, false, null);

            AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder();
            TextFormat.merge(new String(data, UTF_8), builder);
            AuditorVoteFormat v = builder.build();
            String[] parts = v.getBookieId().split(":");
            return new BookieSocketAddress(parts[0],
                Integer.parseInt(parts[1]));
        } catch (KeeperException | ParseException e) {
            throw new MetadataException(
                Code.METADATA_SERVICE_ERROR,
                "Failed to get current auditor under '" + basePath + "'",
                e
            );
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new MetadataException(
                Code.METADATA_SERVICE_ERROR,
                "Interrupted at getting current auditor under '" + basePath + "'",
                ie
            );
        }
    }

    @Override
    public Future<?> select(SelectorListener listener)
        throws MetadataException {
        if (running.compareAndSet(false, true)) {
            try {
                createElectorPath();
            } catch (KeeperException e) {
                throw new MetadataException(
                    Code.METADATA_SERVICE_ERROR,
                    "Failed to create elector path at zookeeper", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MetadataException(
                    Code.METADATA_SERVICE_ERROR,
                    "Interrupted at creating elector path at zookeeper", e);
            }
            return submitElectionTask(listener);
        } else {
            return FutureUtils.Void();
        }
    }

    /**
     * Performing the auditor election using the ZooKeeper ephemeral sequential
     * znode. The bookie which has created the least sequential will be elect as
     * Auditor.
     */
    @VisibleForTesting
    Future<?> submitElectionTask(SelectorListener listener) {

        Runnable r = () -> {
            if (!running.get()) {
                return;
            }
            try {
                // creating my vote in zk. Vote format is 'V_numeric'
                createMyVote();
                List<String> children = zkc.getChildren(getVotePath(""), false);

                if (0 >= children.size()) {
                    throw new IllegalArgumentException(
                            "Atleast one bookie server should present to elect the Auditor!");
                }

                // sorting in ascending order of sequential number
                Collections.sort(children, new ElectionComparator());
                String voteNode = StringUtils.substringAfterLast(myVote,
                                                                 PATH_SEPARATOR);

                // starting Auditing service
                if (children.get(AUDITOR_INDEX).equals(voteNode)) {
                    // update the auditor bookie id in the election path. This is
                    // done for debugging purpose
                    AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder()
                        .setBookieId(bookieId);

                    zkc.setData(getVotePath(""),
                                TextFormat.printToString(builder.build()).getBytes(UTF_8), -1);

                    listener.onLeaderSelected();

                } else {
                    // If not an auditor, will be watching to my predecessor and
                    // looking the previous node deletion.
                    Watcher electionWatcher = new ElectionWatcher();
                    int myIndex = children.indexOf(voteNode);
                    int prevNodeIndex = myIndex - 1;
                    if (null == zkc.exists(getVotePath(PATH_SEPARATOR)
                                           + children.get(prevNodeIndex), electionWatcher)) {
                        // While adding, the previous znode doesn't exists.
                        // Again going to election.
                        submitElectionTask(listener);
                    }
                    listener.onSelectionAttempt();
                }
            } catch (IOException | KeeperException e) {
                log.error("Exception while performing auditor election", e);
                submitShutdownTask(listener);
            } catch (InterruptedException e) {
                log.error("Interrupted while performing auditor election", e);
                Thread.currentThread().interrupt();
                submitShutdownTask(listener);
            }
        };
        return executor.submit(r);
    }

    /**
     * Run cleanup operations for the auditor elector.
     */
    private void submitShutdownTask(SelectorListener listener) {
        executor.submit(() -> {
            if (!running.compareAndSet(true, false)) {
                return;
            }
            log.info("Shutting down AuditorElector");
            if (null != listener) {
                listener.onLeaderExpired();
            }
            if (myVote != null) {
                try {
                    zkc.delete(myVote, -1);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("InterruptedException while deleting myVote: {}", myVote,
                             ie);
                } catch (KeeperException ke) {
                    log.error("Exception while deleting myVote: {}", myVote, ke);
                }
            }
        });
    }

    @Override
    public void close() {
        if (executor.isShutdown()) {
            return;
        }
        submitShutdownTask(null);
        executor.shutdown();
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
