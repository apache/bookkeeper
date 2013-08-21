package org.apache.bookkeeper.client;

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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * This class is responsible for maintaining a consistent view of what bookies
 * are available by reading Zookeeper (and setting watches on the bookie nodes).
 * When a bookie fails, the other parts of the code turn to this class to find a
 * replacement
 *
 */
class BookieWatcher implements Watcher, ChildrenCallback {
    static final Logger logger = LoggerFactory.getLogger(BookieWatcher.class);

    // Bookie registration path in ZK
    private final String bookieRegistrationPath;
    static final Set<InetSocketAddress> EMPTY_SET = new HashSet<InetSocketAddress>();
    public static int ZK_CONNECT_BACKOFF_SEC = 1;

    final BookKeeper bk;

    HashSet<InetSocketAddress> knownBookies = new HashSet<InetSocketAddress>();
    final ScheduledExecutorService scheduler;

    SafeRunnable reReadTask = new SafeRunnable() {
        @Override
        public void safeRun() {
            readBookies();
        }
    };
    private ReadOnlyBookieWatcher readOnlyBookieWatcher;

    public BookieWatcher(ClientConfiguration conf,
                         ScheduledExecutorService scheduler,
                         BookKeeper bk) throws KeeperException, InterruptedException  {
        this.bk = bk;
        // ZK bookie registration path
        this.bookieRegistrationPath = conf.getZkAvailableBookiesPath();
        this.scheduler = scheduler;
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

    public Collection<InetSocketAddress> getBookies() throws BKException {
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

    Collection<InetSocketAddress> getReadOnlyBookies() {
        return new HashSet<InetSocketAddress>(readOnlyBookieWatcher.getReadOnlyBookies());
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

        HashSet<InetSocketAddress> newBookieAddrs = convertToBookieAddresses(children);

        final HashSet<InetSocketAddress> deadBookies;
        synchronized (this) {
            deadBookies = (HashSet<InetSocketAddress>)knownBookies.clone();
            deadBookies.removeAll(newBookieAddrs);
            // No need to close readonly bookie clients.
            deadBookies.removeAll(readOnlyBookieWatcher.getReadOnlyBookies());
            knownBookies = newBookieAddrs;
        }

        if (bk.getBookieClient() != null) {
            bk.getBookieClient().closeClients(deadBookies);
        }
    }

    private static HashSet<InetSocketAddress> convertToBookieAddresses(List<String> children) {
        // Read the bookie addresses into a set for efficient lookup
        HashSet<InetSocketAddress> newBookieAddrs = new HashSet<InetSocketAddress>();
        for (String bookieAddrString : children) {
            InetSocketAddress bookieAddr;
            try {
                bookieAddr = StringUtils.parseAddr(bookieAddrString);
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
     * Wrapper over the {@link #getAdditionalBookies(Set, int)} method when there is no exclusion list (or exisiting bookies)
     * @param numBookiesNeeded
     * @return
     * @throws BKNotEnoughBookiesException
     */
    public ArrayList<InetSocketAddress> getNewBookies(int numBookiesNeeded) throws BKNotEnoughBookiesException {
        return getAdditionalBookies(EMPTY_SET, numBookiesNeeded);
    }

    /**
     * Wrapper over the {@link #getAdditionalBookies(Set, int)} method when you just need 1 extra bookie
     * @param existingBookies
     * @return
     * @throws BKNotEnoughBookiesException
     */
    public InetSocketAddress getAdditionalBookie(List<InetSocketAddress> existingBookies)
            throws BKNotEnoughBookiesException {
        return getAdditionalBookies(new HashSet<InetSocketAddress>(existingBookies), 1).get(0);
    }

    /**
     * Returns additional bookies given an exclusion list and how many are needed
     * @param existingBookies
     * @param numAdditionalBookiesNeeded
     * @return
     * @throws BKNotEnoughBookiesException
     */
    public ArrayList<InetSocketAddress> getAdditionalBookies(Set<InetSocketAddress> existingBookies,
            int numAdditionalBookiesNeeded) throws BKNotEnoughBookiesException {

        ArrayList<InetSocketAddress> newBookies = new ArrayList<InetSocketAddress>();

        if (numAdditionalBookiesNeeded <= 0) {
            return newBookies;
        }

        List<InetSocketAddress> allBookies;

        synchronized (this) {
            allBookies = new ArrayList<InetSocketAddress>(knownBookies);
        }

        Collections.shuffle(allBookies);

        for (InetSocketAddress bookie : allBookies) {
            if (existingBookies.contains(bookie)) {
                continue;
            }

            newBookies.add(bookie);
            numAdditionalBookiesNeeded--;

            if (numAdditionalBookiesNeeded == 0) {
                return newBookies;
            }
        }

        throw new BKNotEnoughBookiesException();
    }

    /**
     * Watcher implementation to watch the readonly bookies under
     * &lt;available&gt;/readonly
     */
    private static class ReadOnlyBookieWatcher implements Watcher, ChildrenCallback {

        private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyBookieWatcher.class);
        private HashSet<InetSocketAddress> readOnlyBookies = new HashSet<InetSocketAddress>();
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

            HashSet<InetSocketAddress> newReadOnlyBookies = convertToBookieAddresses(children);
            readOnlyBookies = newReadOnlyBookies;
        }

        // returns the readonly bookies
        public HashSet<InetSocketAddress> getReadOnlyBookies() {
            return readOnlyBookies;
        }
    }
}
