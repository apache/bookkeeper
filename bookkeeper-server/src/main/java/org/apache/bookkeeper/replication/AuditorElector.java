/**
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
package org.apache.bookkeeper.replication;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.replication.ReplicationStats.AUDITOR_SCOPE;
import static org.apache.bookkeeper.replication.ReplicationStats.ELECTION_ATTEMPTS;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ZkLayoutManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.AuditorVoteFormat;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.annotations.StatsDoc;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performing auditor election using Apache ZooKeeper. Using ZooKeeper as a
 * coordination service, when a bookie bids for auditor, it creates an ephemeral
 * sequential file (znode) on ZooKeeper and considered as their vote. Vote
 * format is 'V_sequencenumber'. Election will be done by comparing the
 * ephemeral sequential numbers and the bookie which has created the least znode
 * will be elected as Auditor. All the other bookies will be watching on their
 * predecessor znode according to the ephemeral sequence numbers.
 */
@StatsDoc(
    name = AUDITOR_SCOPE,
    help = "Auditor related stats"
)
public class AuditorElector {
    private static final Logger LOG = LoggerFactory
            .getLogger(AuditorElector.class);
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

    private final String bookieId;
    private final ServerConfiguration conf;
    private final BookKeeper bkc;
    private final ZooKeeper zkc;
    private final boolean ownBkc;
    private final ExecutorService executor;

    private String myVote;
    Auditor auditor;
    private AtomicBoolean running = new AtomicBoolean(false);

    // Expose Stats
    @StatsDoc(
        name = ELECTION_ATTEMPTS,
        help = "The number of auditor election attempts"
    )
    private final Counter electionAttempts;
    private final StatsLogger statsLogger;


    @VisibleForTesting
    public AuditorElector(final String bookieId, ServerConfiguration conf) throws UnavailableException {
        this(
            bookieId,
            conf,
            Auditor.createBookKeeperClientThrowUnavailableException(conf),
            true);
    }

    /**
     * AuditorElector for performing the auditor election.
     *
     * @param bookieId
     *            - bookie identifier, comprises HostAddress:Port
     * @param conf
     *            - configuration
     * @param bkc
     *            - bookkeeper instance
     * @throws UnavailableException
     *             throws unavailable exception while initializing the elector
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          boolean ownBkc) throws UnavailableException {
        this(bookieId, conf, bkc, NullStatsLogger.INSTANCE, ownBkc);
    }

    /**
     * AuditorElector for performing the auditor election.
     *
     * @param bookieId
     *            - bookie identifier, comprises HostAddress:Port
     * @param conf
     *            - configuration
     * @param bkc
     *            - bookkeeper instance
     * @param statsLogger
     *            - stats logger
     * @throws UnavailableException
     *             throws unavailable exception while initializing the elector
     */
    public AuditorElector(final String bookieId,
                          ServerConfiguration conf,
                          BookKeeper bkc,
                          StatsLogger statsLogger,
                          boolean ownBkc) throws UnavailableException {
        this.bookieId = bookieId;
        this.conf = conf;
        this.bkc = bkc;
        this.ownBkc = ownBkc;
        this.zkc = ((ZkLayoutManager) bkc.getMetadataClientDriver().getLayoutManager()).getZk();
        this.statsLogger = statsLogger;
        this.electionAttempts = statsLogger.getCounter(ELECTION_ATTEMPTS);
        basePath = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + '/'
                + BookKeeperConstants.UNDER_REPLICATION_NODE;
        electionPath = basePath + '/' + ELECTION_ZNODE;
        createElectorPath();
        executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "AuditorElector-" + bookieId);
                }
            });
    }

    private void createMyVote() throws KeeperException, InterruptedException {
        if (null == myVote || null == zkc.exists(myVote, false)) {
            List<ACL> zkAcls = ZkUtils.getACLs(conf);
            AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder()
                .setBookieId(bookieId);
            myVote = zkc.create(getVotePath(PATH_SEPARATOR + VOTE_PREFIX),
                    TextFormat.printToString(builder.build()).getBytes(UTF_8), zkAcls,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
        }
    }

    String getMyVote() {
        return myVote;
    }

    private String getVotePath(String vote) {
        return electionPath + vote;
    }

    private void createElectorPath() throws UnavailableException {
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
            throw new UnavailableException(
                    "Failed to initialize Auditor Elector", ke);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new UnavailableException(
                    "Failed to initialize Auditor Elector", ie);
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
                LOG.error("Lost ZK connection, shutting down");
                submitShutdownTask();
            } else if (event.getType() == EventType.NodeDeleted) {
                submitElectionTask();
            }
        }
    }

    public Future<?> start() {
        running.set(true);
        return submitElectionTask();
    }

    /**
     * Run cleanup operations for the auditor elector.
     */
    private void submitShutdownTask() {
        executor.submit(new Runnable() {
                @Override
                public void run() {
                    if (!running.compareAndSet(true, false)) {
                        return;
                    }
                    LOG.info("Shutting down AuditorElector");
                    if (myVote != null) {
                        try {
                            zkc.delete(myVote, -1);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            LOG.warn("InterruptedException while deleting myVote: " + myVote,
                                     ie);
                        } catch (KeeperException ke) {
                            LOG.error("Exception while deleting myVote:" + myVote, ke);
                        }
                    }
                }
            });
    }

    /**
     * Performing the auditor election using the ZooKeeper ephemeral sequential
     * znode. The bookie which has created the least sequential will be elect as
     * Auditor.
     */
    @VisibleForTesting
    Future<?> submitElectionTask() {

        Runnable r = new Runnable() {
                @Override
                public void run() {
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
                            auditor = new Auditor(bookieId, conf, bkc, false, statsLogger);
                            auditor.start();
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
                                submitElectionTask();
                            }
                            electionAttempts.inc();
                        }
                    } catch (KeeperException e) {
                        LOG.error("Exception while performing auditor election", e);
                        submitShutdownTask();
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted while performing auditor election", e);
                        Thread.currentThread().interrupt();
                        submitShutdownTask();
                    } catch (UnavailableException e) {
                        LOG.error("Ledger underreplication manager unavailable during election", e);
                        submitShutdownTask();
                    }
                }
            };
        return executor.submit(r);
    }

    @VisibleForTesting
    Auditor getAuditor() {
        return auditor;
    }

    /**
     * Query zookeeper for the currently elected auditor.
     * @return the bookie id of the current auditor
     */
    public static BookieSocketAddress getCurrentAuditor(ServerConfiguration conf, ZooKeeper zk)
            throws KeeperException, InterruptedException, IOException {
        String electionRoot = ZKMetadataDriverBase.resolveZkLedgersRootPath(conf) + '/'
            + BookKeeperConstants.UNDER_REPLICATION_NODE + '/' + ELECTION_ZNODE;

        List<String> children = zk.getChildren(electionRoot, false);
        Collections.sort(children, new AuditorElector.ElectionComparator());
        if (children.size() < 1) {
            return null;
        }
        String ledger = electionRoot + "/" + children.get(AUDITOR_INDEX);
        byte[] data = zk.getData(ledger, false, null);

        AuditorVoteFormat.Builder builder = AuditorVoteFormat.newBuilder();
        TextFormat.merge(new String(data, UTF_8), builder);
        AuditorVoteFormat v = builder.build();
        String[] parts = v.getBookieId().split(":");
        return new BookieSocketAddress(parts[0],
                                       Integer.parseInt(parts[1]));
    }

    /**
     * Shutting down AuditorElector.
     */
    public void shutdown() throws InterruptedException {
        synchronized (this) {
            if (executor.isShutdown()) {
                return;
            }
            submitShutdownTask();
            executor.shutdown();
        }

        if (auditor != null) {
            auditor.shutdown();
            auditor = null;
        }
        if (ownBkc) {
            try {
                bkc.close();
            } catch (BKException e) {
                LOG.warn("Failed to close bookkeeper client", e);
            }
        }
    }

    /**
     * If current bookie is running as auditor, return the status of the
     * auditor. Otherwise return the status of elector.
     *
     * @return
     */
    public boolean isRunning() {
        if (auditor != null) {
            return auditor.isRunning();
        }
        return running.get();
    }

    @Override
    public String toString() {
        return "AuditorElector for " + bookieId;
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
