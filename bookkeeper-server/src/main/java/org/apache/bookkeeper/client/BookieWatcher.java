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

    // Bookie registration path in ZK
    private final String bookieRegistrationPath;

    final BookKeeper bk;
    final ScheduledExecutorService scheduler;
    final EnsemblePlacementPolicy placementPolicy;

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
            scheduler.schedule(reReadTask, ZK_CONNECT_BACKOFF_SEC, TimeUnit.SECONDS);
            return;
        }

        // Just exclude the 'readonly' znode to exclude r-o bookies from
        // available nodes list.
        children.remove(BookKeeperConstants.READONLY);

        HashSet<BookieSocketAddress> newBookieAddrs = convertToBookieAddresses(children);

        final Set<BookieSocketAddress> deadBookies;
        synchronized (this) {
            Set<BookieSocketAddress> readonlyBookies = readOnlyBookieWatcher.getReadOnlyBookies();
            deadBookies = placementPolicy.onClusterChanged(newBookieAddrs, readonlyBookies);
        }

        if (bk.getBookieClient() != null) {
            bk.getBookieClient().closeClients(deadBookies);
        }
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
        return placementPolicy.newEnsemble(ensembleSize, writeQuorumSize, EMPTY_SET);
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
        return placementPolicy.replaceBookie(addr, new HashSet<BookieSocketAddress>(existingBookies));
    }

    /**
     * Watcher implementation to watch the readonly bookies under
     * &lt;available&gt;/readonly
     */
    private static class ReadOnlyBookieWatcher implements Watcher, ChildrenCallback {

        private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyBookieWatcher.class);
        private HashSet<BookieSocketAddress> readOnlyBookies = new HashSet<BookieSocketAddress>();
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
