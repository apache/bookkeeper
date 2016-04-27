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
package org.apache.bookkeeper.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * This class is responsible for maintaining a consistent view of what bookies
 * are available by reading Zookeeper (and setting watches on the bookie nodes).
 * When a bookie fails, the other parts of the code turn to this class to find a
 * replacement
 *
 */
class BookieWatcher implements Watcher, ChildrenCallback {
    static final Logger logger = LoggerFactory.getLogger(BookieWatcher.class);

    public static int ZK_CONNECT_BACKOFF_SEC = 1;
    private static final Set<BookieSocketAddress> EMPTY_SET = new HashSet<BookieSocketAddress>();
    private static final Boolean BOOLEAN = new Boolean(true);

    // Bookie registration path in ZK
    private final String bookieRegistrationPath;

    final BookKeeper bk;
    final ScheduledExecutorService scheduler;
    final EnsemblePlacementPolicy placementPolicy;

    // Bookies that will not be preferred to be chosen in a new ensemble
    final Cache<BookieSocketAddress, Boolean> quarantinedBookies;

    SafeRunnable reReadTask = new SafeRunnable() {
        @Override
        public void safeRun() {
            readBookies();
        }
    };
    private ReadOnlyBookieWatcher readOnlyBookieWatcher;

    public BookieWatcher(ClientConfiguration conf,
                         ScheduledExecutorService scheduler,
                         EnsemblePlacementPolicy placementPolicy,
                         BookKeeper bk) throws KeeperException, InterruptedException  {
        this.bk = bk;
        // ZK bookie registration path
        this.bookieRegistrationPath = conf.getZkAvailableBookiesPath();
        this.scheduler = scheduler;
        this.placementPolicy = placementPolicy;
        readOnlyBookieWatcher = new ReadOnlyBookieWatcher(conf, bk);
        this.quarantinedBookies = CacheBuilder.newBuilder()
                .expireAfterWrite(conf.getBookieQuarantineTimeSeconds(), TimeUnit.SECONDS)
                .removalListener(new RemovalListener<BookieSocketAddress, Boolean>() {

                    @Override
                    public void onRemoval(RemovalNotification<BookieSocketAddress, Boolean> bookie) {
                        logger.info("Bookie {} is no longer quarantined", bookie.getKey());
                    }

                }).build();
    }

    void notifyBookiesChanged(final BookiesListener listener) throws BKException {
        try {
            bk.getZkHandle().getChildren(this.bookieRegistrationPath,
                    new Watcher() {
                        public void process(WatchedEvent event) {
                            // listen children changed event from ZooKeeper
                            if (event.getType() == EventType.NodeChildrenChanged) {
                                listener.availableBookiesChanged();
                            }
                        }
                    });
        } catch (KeeperException ke) {
            logger.error("Error registering watcher with zookeeper", ke);
            throw new BKException.ZKException();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted registering watcher with zookeeper", ie);
            throw new BKException.BKInterruptedException();
        }
    }

    void notifyReadOnlyBookiesChanged(final BookiesListener listener) throws BKException {
        readOnlyBookieWatcher.notifyBookiesChanged(listener);
    }

    public Collection<BookieSocketAddress> getBookies() throws BKException {
        try {
            List<String> children = bk.getZkHandle().getChildren(this.bookieRegistrationPath, false);
            children.remove(BookKeeperConstants.READONLY);
            return convertToBookieAddresses(children);
        } catch (KeeperException ke) {
            logger.error("Failed to get bookie list : ", ke);
            throw new BKException.ZKException();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted reading bookie list", ie);
            throw new BKException.BKInterruptedException();
        }
    }

    Collection<BookieSocketAddress> getReadOnlyBookies() {
        return new HashSet<BookieSocketAddress>(readOnlyBookieWatcher.getReadOnlyBookies());
    }

    public void readBookies() {
        readBookies(this);
    }

    public void readBookies(ChildrenCallback callback) {
        bk.getZkHandle().getChildren(this.bookieRegistrationPath, this, callback, null);
    }

    @Override
    public void process(WatchedEvent event) {
        readBookies();
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {

        if (rc != KeeperException.Code.OK.intValue()) {
            //logger.error("Error while reading bookies", KeeperException.create(Code.get(rc), path));
            // try the read after a second again
            try {
                scheduler.schedule(reReadTask, ZK_CONNECT_BACKOFF_SEC, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ree) {
                logger.warn("Failed to schedule reading bookies task : ", ree);
            }
            return;
        }

        // Just exclude the 'readonly' znode to exclude r-o bookies from
        // available nodes list.
        children.remove(BookKeeperConstants.READONLY);

        HashSet<BookieSocketAddress> newBookieAddrs = convertToBookieAddresses(children);

        synchronized (this) {
            Set<BookieSocketAddress> readonlyBookies = readOnlyBookieWatcher.getReadOnlyBookies();
            placementPolicy.onClusterChanged(newBookieAddrs, readonlyBookies);
        }

        // we don't need to close clients here, because:
        // a. the dead bookies will be removed from topology, which will not be used in new ensemble.
        // b. the read sequence will be reordered based on znode availability, so most of the reads
        //    will not be sent to them.
        // c. the close here is just to disconnect the channel, which doesn't remove the channel from
        //    from pcbc map. we don't really need to disconnect the channel here, since if a bookie is
        //    really down, PCBC will disconnect itself based on netty callback. if we try to disconnect
        //    here, it actually introduces side-effects on case d.
        // d. closing the client here will affect latency if the bookie is alive but just being flaky
        //    on its znode registration due zookeeper session expire.
        // e. if we want to permanently remove a bookkeeper client, we should watch on the cookies' list.
        // if (bk.getBookieClient() != null) {
        //     bk.getBookieClient().closeClients(deadBookies);
        // }
    }

    private static HashSet<BookieSocketAddress> convertToBookieAddresses(List<String> children) {
        // Read the bookie addresses into a set for efficient lookup
        HashSet<BookieSocketAddress> newBookieAddrs = new HashSet<BookieSocketAddress>();
        for (String bookieAddrString : children) {
            BookieSocketAddress bookieAddr;
            try {
                bookieAddr = new BookieSocketAddress(bookieAddrString);
            } catch (IOException e) {
                logger.error("Could not parse bookie address: " + bookieAddrString + ", ignoring this bookie");
                continue;
            }
            newBookieAddrs.add(bookieAddr);
        }
        return newBookieAddrs;
    }

    /**
     * Blocks until bookies are read from zookeeper, used in the {@link BookKeeper} constructor.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void readBookiesBlocking() throws InterruptedException, KeeperException {
        // Read readonly bookies first
        readOnlyBookieWatcher.readROBookiesBlocking();

        final LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();
        readBookies(new ChildrenCallback() {
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                try {
                    BookieWatcher.this.processResult(rc, path, ctx, children);
                    queue.put(rc);
                } catch (InterruptedException e) {
                    logger.error("Interruped when trying to read bookies in a blocking fashion");
                    throw new RuntimeException(e);
                }
            }
        });
        int rc = queue.take();

        if (rc != KeeperException.Code.OK.intValue()) {
            throw KeeperException.create(Code.get(rc));
        }
    }

    /**
     * Create an ensemble with given <i>ensembleSize</i> and <i>writeQuorumSize</i>.
     *
     * @param ensembleSize
     *          Ensemble Size
     * @param writeQuorumSize
     *          Write Quorum Size
     * @return list of bookies for new ensemble.
     * @throws BKNotEnoughBookiesException
     */
    public ArrayList<BookieSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize)
            throws BKNotEnoughBookiesException {
        try {
            // we try to only get from the healthy bookies first
            return placementPolicy.newEnsemble(ensembleSize, writeQuorumSize, new HashSet<BookieSocketAddress>(
                    quarantinedBookies.asMap().keySet()));
        } catch (BKNotEnoughBookiesException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            return placementPolicy.newEnsemble(ensembleSize, writeQuorumSize, EMPTY_SET);
        }
    }

    /**
     * Choose a bookie to replace bookie <i>bookieIdx</i> in <i>existingBookies</i>.
     * @param existingBookies
     *          list of existing bookies.
     * @param bookieIdx
     *          index of the bookie in the list to be replaced.
     * @return the bookie to replace.
     * @throws BKNotEnoughBookiesException
     */
    public BookieSocketAddress replaceBookie(List<BookieSocketAddress> existingBookies, int bookieIdx)
            throws BKNotEnoughBookiesException {
        BookieSocketAddress addr = existingBookies.get(bookieIdx);
        try {
            // we exclude the quarantined bookies also first
            Set<BookieSocketAddress> existingAndQuarantinedBookies = new HashSet<BookieSocketAddress>(existingBookies);
            existingAndQuarantinedBookies.addAll(quarantinedBookies.asMap().keySet());
            return placementPolicy.replaceBookie(addr, existingAndQuarantinedBookies);
        } catch (BKNotEnoughBookiesException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Not enough healthy bookies available, using quarantined bookies");
            }
            return placementPolicy.replaceBookie(addr, new HashSet<BookieSocketAddress>(existingBookies));
        }
    }

    /**
     * Quarantine <i>bookie</i> so it will not be preferred to be chosen for new ensembles.
     * @param bookie
     * @return
     */
    public void quarantineBookie(BookieSocketAddress bookie) {
        if (quarantinedBookies.getIfPresent(bookie) == null) {
            quarantinedBookies.put(bookie, BOOLEAN);
            logger.warn("Bookie {} has been quarantined because of read/write errors.", bookie);
        }
    }

    /**
     * Watcher implementation to watch the readonly bookies under
     * &lt;available&gt;/readonly
     */
    private static class ReadOnlyBookieWatcher implements Watcher, ChildrenCallback {

        private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyBookieWatcher.class);
        private volatile HashSet<BookieSocketAddress> readOnlyBookies = new HashSet<BookieSocketAddress>();
        private BookKeeper bk;
        private String readOnlyBookieRegPath;

        public ReadOnlyBookieWatcher(ClientConfiguration conf, BookKeeper bk) throws KeeperException,
                InterruptedException {
            this.bk = bk;
            readOnlyBookieRegPath = conf.getZkAvailableBookiesPath() + "/"
                    + BookKeeperConstants.READONLY;
            if (null == bk.getZkHandle().exists(readOnlyBookieRegPath, false)) {
                try {
                    bk.getZkHandle().create(readOnlyBookieRegPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } catch (NodeExistsException e) {
                    // this node is just now created by someone.
                }
            }
        }

        @Override
        public void process(WatchedEvent event) {
            readROBookies();
        }

        // read the readonly bookies in blocking fashion. Used only for first
        // time.
        void readROBookiesBlocking() throws InterruptedException, KeeperException {

            final LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();
            readROBookies(new ChildrenCallback() {
                public void processResult(int rc, String path, Object ctx, List<String> children) {
                    try {
                        ReadOnlyBookieWatcher.this.processResult(rc, path, ctx, children);
                        queue.put(rc);
                    } catch (InterruptedException e) {
                        logger.error("Interruped when trying to read readonly bookies in a blocking fashion");
                        throw new RuntimeException(e);
                    }
                }
            });
            int rc = queue.take();

            if (rc != KeeperException.Code.OK.intValue()) {
                throw KeeperException.create(Code.get(rc));
            }
        }

        void notifyBookiesChanged(final BookiesListener listener) throws BKException {
            try {
                List<String> children = bk.getZkHandle().getChildren(this.readOnlyBookieRegPath, new Watcher() {
                    public void process(WatchedEvent event) {
                        // listen children changed event from ZooKeeper
                        if (event.getType() == EventType.NodeChildrenChanged) {
                            listener.availableBookiesChanged();
                        }
                    }
                });

                // Update the list of read-only bookies
                HashSet<BookieSocketAddress> newReadOnlyBookies = convertToBookieAddresses(children);
                readOnlyBookies = newReadOnlyBookies;
            } catch (KeeperException ke) {
                logger.error("Error registering watcher with zookeeper", ke);
                throw new BKException.ZKException();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                logger.error("Interrupted registering watcher with zookeeper", ie);
                throw new BKException.BKInterruptedException();
            }
        }

        // Read children and register watcher for readonly bookies path
        void readROBookies(ChildrenCallback callback) {
            bk.getZkHandle().getChildren(this.readOnlyBookieRegPath, this, callback, null);
        }

        void readROBookies() {
            readROBookies(this);
        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            if (rc != Code.OK.intValue()) {
                LOG.error("Not able to read readonly bookies : ", KeeperException.create(Code.get(rc)));
                return;
            }

            HashSet<BookieSocketAddress> newReadOnlyBookies = convertToBookieAddresses(children);
            readOnlyBookies = newReadOnlyBookies;
        }

        // returns the readonly bookies
        public HashSet<BookieSocketAddress> getReadOnlyBookies() {
            return readOnlyBookies;
        }
    }
}
