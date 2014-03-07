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
package org.apache.hedwig.server.topics;

import static com.google.common.base.Charsets.UTF_8;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.HedwigSocketAddress;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback;
import org.apache.hedwig.zookeeper.SafeAsyncZKCallback.StatCallback;
import org.apache.hedwig.zookeeper.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper based hub server manager.
 */
class ZkHubServerManager implements HubServerManager {

    static Logger logger = LoggerFactory.getLogger(ZkHubServerManager.class);

    final Random rand = new Random();

    private final ServerConfiguration conf;
    private final ZooKeeper zk;
    private final HedwigSocketAddress addr;
    private final TopicManager tm;
    private final String ephemeralNodePath;
    private final String hubNodesPath;

    // hub info structure represent itself
    protected HubInfo myHubInfo;
    protected volatile boolean isSuspended = false;
    protected ManagerListener listener = null;
    protected final ScheduledExecutorService executor;

    // upload hub server load to zookeeper
    StatCallback loadReportingStatCallback = new StatCallback() {
        @Override
        public void safeProcessResult(int rc, String path, Object ctx, Stat stat) {
            if (rc != KeeperException.Code.OK.intValue()) {
                logger.warn("Failed to update load information of hub {} in zk", myHubInfo);
            }
        }
    };

    /**
     * Watcher to monitor available hub server list.
     */
    class ZkHubsWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType().equals(Watcher.Event.EventType.None)) {
                if (event.getState().equals(
                        Watcher.Event.KeeperState.Disconnected)) {
                    logger.warn("ZK client has been disconnected to the ZK server!");
                    isSuspended = true;
                    if (null != listener) {
                        listener.onSuspend();
                    }
                } else if (event.getState().equals(
                        Watcher.Event.KeeperState.SyncConnected)) {
                    if (isSuspended) {
                        logger.info("ZK client has been reconnected to the ZK server!");
                    }
                    isSuspended = false;
                    if (null != listener) {
                        listener.onResume();
                    }
                }
            }
            if (event.getState().equals(Watcher.Event.KeeperState.Expired)) {
                logger.error("ZK client connection to the ZK server has expired.!");
                if (null != listener) {
                    // Shutdown our executor NOW!
                    executor.shutdownNow();
                    listener.onShutdown();
                }
            }
        }
    }

    class RebalanceRunnable implements Runnable {
        private final double tolerancePercentage;
        private final HubLoad maxLoadToShed;
        private final long delaySeconds;

        public RebalanceRunnable(double tolerancePercentage,
                                 HubLoad maxLoadToShed,
                                 long delaySeconds) {
            this.tolerancePercentage = tolerancePercentage;
            this.maxLoadToShed = maxLoadToShed;
            this.delaySeconds = delaySeconds;
        }

        @Override
        public void run() {
            // If we are in suspended state, don't attempt a rebalance.
            if (isSuspended) {
                executor.schedule(this, delaySeconds, TimeUnit.SECONDS);
                return;
            }
            // We should attempt a rebalance. We reschedule the job at the tail so that
            // two rebalances don't happen simultaneously.
            rebalanceCluster(tolerancePercentage, maxLoadToShed, new Callback<Boolean>() {
                private void reschedule(Runnable task) {
                    executor.schedule(task, delaySeconds, TimeUnit.SECONDS);
                }

                @Override
                public void operationFinished(Object ctx, Boolean didRebalance) {
                    if (didRebalance == true) {
                        logger.info("The attempt to rebalance the cluster was successful");
                    } else {
                        logger.info("There was no need to rebalance.");
                    }
                    // Our original runnable was passed as the context.
                    reschedule((Runnable)ctx);
                }

                @Override
                public void operationFailed(Object ctx, PubSubException e) {
                    logger.error("The attempt to rebalance the cluster did not succeed.", e);
                    // Reschedule the job
                    reschedule((Runnable)ctx);
                }
            }, this);
        }

        public void start() {
            // Initiate only if delaySeconds > 0
            if (delaySeconds > 0) {
                logger.info("Starting the rebalancer thread with tolerance={}, maxLoadToShed={} and delay={}",
                    new Object[] { tolerancePercentage, maxLoadToShed.getNumTopics(), delaySeconds });
                executor.schedule(this, delaySeconds, TimeUnit.SECONDS);
            }
        }
    }

    public ZkHubServerManager(ServerConfiguration conf,
                              ZooKeeper zk,
                              HedwigSocketAddress addr,
                              TopicManager tm) {
        this.conf = conf;
        this.zk = zk;
        this.addr = addr;
        this.tm = tm;
        // znode path to store all available hub servers
        this.hubNodesPath = this.conf.getZkHostsPrefix(new StringBuilder()).toString();
        // the node's ephemeral node path
        this.ephemeralNodePath = getHubZkNodePath(addr);
        this.executor = Executors.newSingleThreadScheduledExecutor();
        // register available hub servers list watcher
        zk.register(new ZkHubsWatcher());

        // Start the rebalancer here.
        new RebalanceRunnable(conf.getRebalanceTolerance(), conf.getRebalanceMaxShed(),
                              conf.getRebalanceInterval()).start();
    }

    @Override
    public void registerListener(ManagerListener listener) {
        this.listener = listener;
    }

    /**
     * Get the znode path identifying a hub server.
     *
     * @param node
     *          Hub Server Address
     * @return znode path identifying the hub server.
     */
    private String getHubZkNodePath(HedwigSocketAddress node) {
        String nodePath = this.conf.getZkHostsPrefix(new StringBuilder())
                          .append("/").append(node).toString();
        return nodePath;
    }

    @Override
    public void registerSelf(final HubLoad selfData, final Callback<HubInfo> callback, Object ctx) {
        byte[] loadDataBytes = selfData.toString().getBytes(UTF_8);
        ZkUtils.createFullPathOptimistic(zk, ephemeralNodePath, loadDataBytes, Ids.OPEN_ACL_UNSAFE,
                                         CreateMode.EPHEMERAL, new SafeAsyncZKCallback.StringCallback() {
            @Override
            public void safeProcessResult(int rc, String path, Object ctx, String name) {
                if (rc == Code.OK.intValue()) {
                    // now we are here
                    zk.exists(ephemeralNodePath, false, new SafeAsyncZKCallback.StatCallback() {
                        @Override
                        public void safeProcessResult(int rc, String path, Object ctx, Stat stat) {
                            if (rc == Code.OK.intValue()) {
                                myHubInfo = new HubInfo(addr, stat.getCzxid());
                                callback.operationFinished(ctx, myHubInfo);
                                return;
                            } else {
                                callback.operationFailed(ctx,
                                        new PubSubException.ServiceDownException(
                                        "I can't state my hub node after I created it : "
                                        + ephemeralNodePath));
                                return;
                            }
                        }
                    }, ctx);
                    return;
                }
                if (rc != Code.NODEEXISTS.intValue()) {
                    KeeperException ke = ZkUtils.logErrorAndCreateZKException(
                            "Could not create ephemeral node to register hub", ephemeralNodePath, rc);
                    callback.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                    return;
                }

                logger.info("Found stale ephemeral node while registering hub with ZK, deleting it");

                // Node exists, lets try to delete it and retry
                zk.delete(ephemeralNodePath, -1, new SafeAsyncZKCallback.VoidCallback() {
                    @Override
                    public void safeProcessResult(int rc, String path, Object ctx) {
                        if (rc == Code.OK.intValue() || rc == Code.NONODE.intValue()) {
                            registerSelf(selfData, callback, ctx);
                            return;
                        }
                        KeeperException ke = ZkUtils.logErrorAndCreateZKException(
                                "Could not delete stale ephemeral node to register hub", ephemeralNodePath, rc);
                        callback.operationFailed(ctx, new PubSubException.ServiceDownException(ke));
                        return;
                    }
                }, ctx);
            }
        }, ctx);
    }

    @Override
    public void unregisterSelf() throws IOException {
        try {
            zk.delete(ephemeralNodePath, -1);
        } catch (InterruptedException e) {
            throw new IOException(e);
        } catch (KeeperException e) {
            throw new IOException(e);
        }
    }


    @Override
    public void uploadSelfLoadData(HubLoad selfLoad) {
        logger.debug("Reporting hub load of {} : {}", myHubInfo, selfLoad);
        byte[] loadDataBytes = selfLoad.toString().getBytes(UTF_8);
        zk.setData(ephemeralNodePath, loadDataBytes, -1,
                   loadReportingStatCallback, null);
    }

    @Override
    public void isHubAlive(final HubInfo hub, final Callback<Boolean> callback, Object ctx) {
        zk.exists(getHubZkNodePath(hub.getAddress()), false, new SafeAsyncZKCallback.StatCallback() {
            @Override
            public void safeProcessResult(int rc, String path, Object ctx, Stat stat) {
                if (rc == Code.NONODE.intValue()) {
                    callback.operationFinished(ctx, false);
                } else if (rc == Code.OK.intValue()) {
                    if (hub.getZxid() == stat.getCzxid()) {
                        callback.operationFinished(ctx, true);
                    } else {
                        callback.operationFinished(ctx, false);
                    }
                } else {
                    callback.operationFailed(ctx, new PubSubException.ServiceDownException(
                        "Failed to check whether hub server " + hub + " is alive!"));
                }
            }
        }, ctx);
    }

    @Override
    public void chooseLeastLoadedHub(final Callback<HubInfo> callback, Object ctx) {
        // Get the list of existing hosts
        zk.getChildren(hubNodesPath, false, new SafeAsyncZKCallback.ChildrenCallback() {
            @Override
            public void safeProcessResult(int rc, String path, Object ctx,
                                          List<String> children) {
                if (rc != Code.OK.intValue()) {
                    KeeperException e = ZkUtils.logErrorAndCreateZKException(
                        "Could not get list of available hubs", path, rc);
                    callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                    return;
                }
                chooseLeastLoadedNode(children, callback, ctx);
            }
        }, ctx);
    }

    private void chooseLeastLoadedNode(final List<String> children,
                                       final Callback<HubInfo> callback, Object ctx) {
        SafeAsyncZKCallback.DataCallback dataCallback = new SafeAsyncZKCallback.DataCallback() {
            int numResponses = 0;
            HubLoad minLoad = HubLoad.MAX_LOAD;
            String leastLoaded = null;
            long leastLoadedCzxid = 0;

            @Override
            public void safeProcessResult(int rc, String path, Object ctx,
                                          byte[] data, Stat stat) {
                synchronized (this) {
                    if (rc == KeeperException.Code.OK.intValue()) {
                        try {
                            HubLoad load = HubLoad.parse(new String(data, UTF_8));
                            logger.debug("Found server {} with load: {}", ctx, load);
                            int compareRes = load.compareTo(minLoad);
                            if (compareRes < 0 || (compareRes == 0 && rand.nextBoolean())) {
                                minLoad = load;
                                leastLoaded = (String) ctx;
                                leastLoadedCzxid = stat.getCzxid();
                            }
                        } catch (HubLoad.InvalidHubLoadException e) {
                            logger.warn("Corrupted load information from hub : " + ctx);
                            // some corrupted data, we'll just ignore this hub
                        }
                    }
                    numResponses++;

                    if (numResponses == children.size()) {
                        if (leastLoaded == null) {
                            callback.operationFailed(ctx,
                                new PubSubException.ServiceDownException("No hub available"));
                            return;
                        }
                        try {
                            HedwigSocketAddress owner = new HedwigSocketAddress(leastLoaded);
                            callback.operationFinished(ctx, new HubInfo(owner, leastLoadedCzxid));
                        } catch (Throwable t) {
                            callback.operationFailed(ctx,
                                new PubSubException.ServiceDownException("Least loaded hub server "
                                                                       + leastLoaded + " is invalid."));
                        }
                    }
                }
            }
        };

        for (String child : children) {
            zk.getData(conf.getZkHostsPrefix(new StringBuilder()).append("/").append(child).toString(), false,
                       dataCallback, child);
        }
    }

    /**
     * Get a map of all currently active hubs with their advertised load.
     * @param callback
     * @param originalCtx
     */
    private void getActiveHubsInfoWithLoad(final Callback<Map<HubInfo, HubLoad>> callback,
                                           final Object originalCtx) {
        // Get the list of children and then for each child, get the data. All asynchronously.
        zk.getChildren(hubNodesPath, false, new SafeAsyncZKCallback.ChildrenCallback() {
            @Override
            public void safeProcessResult(int rc, String path, Object ctx, final List<String> children) {
                if (rc != Code.OK.intValue()) {
                    KeeperException e = ZkUtils.logErrorAndCreateZKException(
                            "Could not get children for given path", path, rc);
                    callback.operationFailed(ctx, new PubSubException.ServiceDownException(e));
                    return;
                }

                // The data callback for every child node
                SafeAsyncZKCallback.DataCallback dataCallback = new SafeAsyncZKCallback.DataCallback() {
                    Map<HubInfo, HubLoad> loadMap = new HashMap<HubInfo, HubLoad>();
                    int numResponse = 0;
                    @Override
                    public void safeProcessResult(int rc, String path, Object dataCtx,
                                                  byte[] data, Stat stat) {
                        synchronized (this) {
                            if (rc == Code.OK.intValue()) {
                                // Put this load in the map. dataCtx is actually the child string which is the
                                // IP:PORT:SSL representation of the hub.
                                try {
                                    HubInfo hubInfo =
                                        new HubInfo(new HedwigSocketAddress((String)dataCtx), stat.getCzxid());
                                    HubLoad hubLoad = HubLoad.parse(new String(data, UTF_8));
                                    this.loadMap.put(hubInfo, hubLoad);
                                } catch (HubLoad.InvalidHubLoadException e) {
                                    logger.warn("Corrupt data found for a hub. Ignoring.");
                                }
                            }
                            numResponse++;
                            if (numResponse == children.size()) {
                                // We got less number of valid responses than the hubs we saw previously.
                                // Signal an error.
                                if (loadMap.size() != numResponse) {
                                    callback.operationFailed(originalCtx,
                                        new PubSubException.UnexpectedConditionException(
                                           "Fewer OK responses than the number of active hubs seen previously."));
                                    return;
                                }
                                // We've seen all responses. All OK.
                                callback.operationFinished(originalCtx, loadMap);
                            }
                        }
                    }
                };

                for (String child : children) {
                    String znode = conf.getZkHostsPrefix(new StringBuilder()).append("/").append(child).toString();
                    zk.getData(znode, false, dataCallback, child);
                }
            }
        }, originalCtx);
    }

    @Override
    public void rebalanceCluster(final double tolerancePercentage, final HubLoad maxLoadToShed,
                                 final Callback<Boolean> callback, final Object ctx) {
        // Get the load on all active hubs and then shed load if required.
        getActiveHubsInfoWithLoad(new Callback<Map<HubInfo, HubLoad>>() {
            @Override
            public void operationFinished(Object ctx, Map<HubInfo, HubLoad> loadMap) {
                if (null == tm) {
                    // No topic manager, so no load to shed.
                    callback.operationFinished(ctx, false);
                    return;
                }
                TopicBasedLoadShedder tbls = new TopicBasedLoadShedder(tm,
                        tolerancePercentage, maxLoadToShed);
                tbls.shedLoad(loadMap, callback, ctx);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                // Rebalance failed. Log this and signal failure on the callback.
                logger.error("Failed to get active hubs. Cannot attempt a rebalance.");
                callback.operationFailed(ctx, e);
            }
        }, ctx);
    }


}
