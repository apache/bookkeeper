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
package org.apache.distributedlog.impl.federated;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.common.concurrent.FutureEventListener;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.ZooKeeperClient;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.exceptions.LogExistsException;
import org.apache.distributedlog.exceptions.UnexpectedException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.ZKNamespaceWatcher;
import org.apache.distributedlog.metadata.LogMetadataStore;
import org.apache.distributedlog.namespace.NamespaceWatcher;
import org.apache.distributedlog.util.Utils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * A Federated ZooKeeper Based Log Metadata Store.
 *To Upgrade a simple ZKLogMetadataStore to FederatedZKLogMetadataStore, following steps should be taken in sequence:
 * a) deploy the new code with disabling createStreamsIfNotExists in all writer.
 * b) once all proxies disable the flag, update namespace binding to enable federated namespace.
 * c) restart writers to take federated namespace in place.
 *
 * <p>NOTE: current federated namespace isn't optimized for deletion/creation. so don't use it in the workloads
 *       that have lots of creations or deletions.</p>
 */
public class FederatedZKLogMetadataStore
        extends NamespaceWatcher
        implements LogMetadataStore, Watcher, Runnable, FutureEventListener<Set<URI>> {

    private static final Logger logger = LoggerFactory.getLogger(FederatedZKLogMetadataStore.class);

    private static final  String ZNODE_SUB_NAMESPACES = ".subnamespaces";
    private static final  String SUB_NAMESPACE_PREFIX = "NS_";

    /**
     * Create the federated namespace.
     *
     * @param namespace
     *          namespace to create
     * @param zkc
     *          zookeeper client
     * @throws InterruptedException
     * @throws ZooKeeperClient.ZooKeeperConnectionException
     * @throws KeeperException
     */
    public static void createFederatedNamespace(URI namespace, ZooKeeperClient zkc)
            throws IOException, KeeperException {
        String zkSubNamespacesPath = namespace.getPath() + "/" + ZNODE_SUB_NAMESPACES;
        Utils.zkCreateFullPathOptimistic(zkc, zkSubNamespacesPath, new byte[0],
                zkc.getDefaultACL(), CreateMode.PERSISTENT);
    }

    /**
     * Represent a sub namespace inside the federated namespace.
     */
    class SubNamespace implements NamespaceListener {
        final URI uri;
        final ZKNamespaceWatcher watcher;
        CompletableFuture<Set<String>> logsFuture = new CompletableFuture<Set<String>>();

        SubNamespace(URI uri) {
            this.uri = uri;
            this.watcher = new ZKNamespaceWatcher(conf, uri, zkc, scheduler);
            this.watcher.registerListener(this);
        }

        void watch() {
            this.watcher.watchNamespaceChanges();
        }

        synchronized CompletableFuture<Set<String>> getLogs() {
            return logsFuture;
        }

        @Override
        public void onStreamsChanged(Iterator<String> newLogsIter) {
            Set<String> newLogs = Sets.newHashSet(newLogsIter);
            Set<String> oldLogs = Sets.newHashSet();

            // update the sub namespace cache
            CompletableFuture<Set<String>> newLogsPromise;
            synchronized (this) {
                if (logsFuture.isDone()) { // the promise is already satisfied
                    try {
                        oldLogs = FutureUtils.result(logsFuture);
                    } catch (Exception e) {
                        logger.error("Unexpected exception when getting logs from a satisified future of {} : ",
                                uri, e);
                    }
                    logsFuture = new CompletableFuture<Set<String>>();
                }

                // update the reverse cache
                for (String logName : newLogs) {
                    URI oldURI = log2Locations.putIfAbsent(logName, uri);
                    if (null != oldURI && !Objects.equal(uri, oldURI)) {
                        logger.error("Log {} is found duplicated in multiple locations : old location = {},"
                                + " new location = {}", logName, oldURI, uri);
                        duplicatedLogFound.set(true);
                    }
                }

                // remove the gone streams
                Set<String> deletedLogs = Sets.difference(oldLogs, newLogs);
                for (String logName : deletedLogs) {
                    log2Locations.remove(logName, uri);
                }
                newLogsPromise = logsFuture;
            }
            newLogsPromise.complete(newLogs);

            // notify namespace changes
            notifyOnNamespaceChanges();
        }
    }

    final DistributedLogConfiguration conf;
    final URI namespace;
    final ZooKeeperClient zkc;
    final OrderedScheduler scheduler;
    final String zkSubnamespacesPath;
    final AtomicBoolean duplicatedLogFound = new AtomicBoolean(false);
    final AtomicReference<String> duplicatedLogName = new AtomicReference<String>(null);
    final AtomicReference<Integer> zkSubnamespacesVersion = new AtomicReference<Integer>(null);

    final int maxLogsPerSubnamespace;
    // sub namespaces
    final ConcurrentSkipListMap<URI, SubNamespace> subNamespaces;
    // map between log name and its location
    final ConcurrentMap<String, URI> log2Locations;
    // final
    final boolean forceCheckLogExistence;

    public FederatedZKLogMetadataStore(
            DistributedLogConfiguration conf,
            URI namespace,
            ZooKeeperClient zkc,
            OrderedScheduler scheduler) throws IOException {
        this.conf = conf;
        this.namespace = namespace;
        this.zkc = zkc;
        this.scheduler = scheduler;
        this.forceCheckLogExistence = conf.getFederatedCheckExistenceWhenCacheMiss();
        this.subNamespaces = new ConcurrentSkipListMap<URI, SubNamespace>();
        this.log2Locations = new ConcurrentHashMap<String, URI>();
        this.zkSubnamespacesPath = namespace.getPath() + "/" + ZNODE_SUB_NAMESPACES;
        this.maxLogsPerSubnamespace = conf.getFederatedMaxLogsPerSubnamespace();

        // fetch the sub namespace
        Set<URI> uris;
        try {
            uris = FutureUtils.result(fetchSubNamespaces(this));
        } catch (Exception e) {
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException(e);
            }
        }
        for (URI uri : uris) {
            SubNamespace subNs = new SubNamespace(uri);
            if (null == subNamespaces.putIfAbsent(uri, subNs)) {
                subNs.watch();
                logger.info("Watched sub namespace {}", uri);
            }
        }

        logger.info("Federated ZK LogMetadataStore is initialized for {}", namespace);
    }

    private void scheduleTask(Runnable r, long ms) {
        if (duplicatedLogFound.get()) {
            logger.error("Scheduler is halted for federated namespace {} as duplicated log found",
                    namespace);
            return;
        }
        try {
            scheduler.schedule(r, ms, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ree) {
            logger.error("Task {} scheduled in {} ms is rejected : ", r, ms, ree);
        }
    }

    private <T> CompletableFuture<T> postStateCheck(CompletableFuture<T> future) {
        final CompletableFuture<T> postCheckedPromise = new CompletableFuture<T>();
        future.whenComplete(new FutureEventListener<T>() {
            @Override
            public void onSuccess(T value) {
                if (duplicatedLogFound.get()) {
                    postCheckedPromise.completeExceptionally(
                            new UnexpectedException("Duplicate log found under " + namespace));
                } else {
                    postCheckedPromise.complete(value);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                postCheckedPromise.completeExceptionally(cause);
            }
        });
        return postCheckedPromise;
    }

    //
    // SubNamespace Related Methods
    //

    @VisibleForTesting
    Set<URI> getSubnamespaces() {
        return subNamespaces.keySet();
    }

    @VisibleForTesting
    void removeLogFromCache(String logName) {
        log2Locations.remove(logName);
    }

    private URI getSubNamespaceURI(String ns) throws URISyntaxException {
        return new URI(
                namespace.getScheme(),
                namespace.getUserInfo(),
                namespace.getHost(),
                namespace.getPort(),
                namespace.getPath() + "/" + ZNODE_SUB_NAMESPACES + "/" + ns,
                namespace.getQuery(),
                namespace.getFragment());
    }

    CompletableFuture<Set<URI>> getCachedSubNamespaces() {
        Set<URI> nsSet = subNamespaces.keySet();
        return FutureUtils.value(nsSet);
    }

    CompletableFuture<Set<URI>> fetchSubNamespaces(final Watcher watcher) {
        final CompletableFuture<Set<URI>> promise = new CompletableFuture<Set<URI>>();
        try {
            zkc.get().sync(this.zkSubnamespacesPath, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    if (Code.OK.intValue() == rc) {
                        fetchSubNamespaces(watcher, promise);
                    } else {
                        promise.completeExceptionally(KeeperException.create(Code.get(rc)));
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.completeExceptionally(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            promise.completeExceptionally(e);
        }
        return promise;
    }

    private void fetchSubNamespaces(Watcher watcher,
                                    final CompletableFuture<Set<URI>> promise) {
        try {
            zkc.get().getChildren(this.zkSubnamespacesPath, watcher,
                    new AsyncCallback.Children2Callback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                            if (Code.NONODE.intValue() == rc) {
                                promise.completeExceptionally(new UnexpectedException(
                                        "The subnamespaces don't exist for the federated namespace " + namespace));
                            } else if (Code.OK.intValue() == rc) {
                                Set<URI> subnamespaces = Sets.newHashSet();
                                subnamespaces.add(namespace);
                                try {
                                    for (String ns : children) {
                                        subnamespaces.add(getSubNamespaceURI(ns));
                                    }
                                } catch (URISyntaxException use) {
                                    logger.error("Invalid sub namespace uri found : ", use);
                                    promise.completeExceptionally(new UnexpectedException(
                                            "Invalid sub namespace uri found in " + namespace, use));
                                    return;
                                }
                                // update the sub namespaces set before update version
                                setZkSubnamespacesVersion(stat.getVersion());
                                promise.complete(subnamespaces);
                            }
                        }
                    }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.completeExceptionally(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            promise.completeExceptionally(e);
        }
    }

    @Override
    public void run() {
        fetchSubNamespaces(this).whenComplete(this);
    }

    @Override
    public void onSuccess(Set<URI> uris) {
        for (URI uri : uris) {
            if (subNamespaces.containsKey(uri)) {
                continue;
            }
            SubNamespace subNs = new SubNamespace(uri);
            if (null == subNamespaces.putIfAbsent(uri, subNs)) {
                subNs.watch();
                logger.info("Watched new sub namespace {}.", uri);
                notifyOnNamespaceChanges();
            }
        }
    }

    @Override
    public void onFailure(Throwable cause) {
        // failed to fetch namespaces, retry later
        scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (Event.EventType.None == watchedEvent.getType()
                && Event.KeeperState.Expired == watchedEvent.getState()) {
            scheduleTask(this, conf.getZKSessionTimeoutMilliseconds());
            return;
        }
        if (Event.EventType.NodeChildrenChanged == watchedEvent.getType()) {
            // fetch the namespace
            fetchSubNamespaces(this).whenComplete(this);
        }
    }

    //
    // Log Related Methods
    //

    private <T> CompletableFuture<T> duplicatedLogException(String logName) {
        return FutureUtils.exception(new UnexpectedException("Duplicated log " + logName
                + " found in namespace " + namespace));
    }

    @Override
    public CompletableFuture<URI> createLog(final String logName) {
        if (duplicatedLogFound.get()) {
            return duplicatedLogException(duplicatedLogName.get());
        }
        CompletableFuture<URI> createPromise = new CompletableFuture<URI>();
        doCreateLog(logName, createPromise);
        return postStateCheck(createPromise);
    }

    void doCreateLog(final String logName, final CompletableFuture<URI> createPromise) {
        getLogLocation(logName).whenComplete(new FutureEventListener<Optional<URI>>() {
            @Override
            public void onSuccess(Optional<URI> uriOptional) {
                if (uriOptional.isPresent()) {
                    createPromise.completeExceptionally(
                            new LogExistsException("Log " + logName + " already exists in " + uriOptional.get()));
                } else {
                    getCachedSubNamespacesAndCreateLog(logName, createPromise);
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                createPromise.completeExceptionally(cause);
            }
        });
    }

    private void getCachedSubNamespacesAndCreateLog(final String logName,
                                                    final CompletableFuture<URI> createPromise) {
        getCachedSubNamespaces().whenComplete(new FutureEventListener<Set<URI>>() {
            @Override
            public void onSuccess(Set<URI> uris) {
                findSubNamespaceToCreateLog(logName, uris, createPromise);
            }

            @Override
            public void onFailure(Throwable cause) {
                createPromise.completeExceptionally(cause);
            }
        });
    }

    private void fetchSubNamespacesAndCreateLog(final String logName,
                                                final CompletableFuture<URI> createPromise) {
        fetchSubNamespaces(null).whenComplete(new FutureEventListener<Set<URI>>() {
            @Override
            public void onSuccess(Set<URI> uris) {
                findSubNamespaceToCreateLog(logName, uris, createPromise);
            }

            @Override
            public void onFailure(Throwable cause) {
                createPromise.completeExceptionally(cause);
            }
        });
    }

    private void findSubNamespaceToCreateLog(final String logName,
                                             final Set<URI> uris,
                                             final CompletableFuture<URI> createPromise) {
        final List<URI> uriList = Lists.newArrayListWithExpectedSize(uris.size());
        List<CompletableFuture<Set<String>>> futureList = Lists.newArrayListWithExpectedSize(uris.size());
        for (URI uri : uris) {
            SubNamespace subNs = subNamespaces.get(uri);
            if (null == subNs) {
                createPromise.completeExceptionally(new UnexpectedException("No sub namespace " + uri + " found"));
                return;
            }
            futureList.add(subNs.getLogs());
            uriList.add(uri);
        }
        FutureUtils.collect(futureList).whenComplete(new FutureEventListener<List<Set<String>>>() {
            @Override
            public void onSuccess(List<Set<String>> resultList) {
                for (int i = resultList.size() - 1; i >= 0; i--) {
                    Set<String> logs = resultList.get(i);
                    if (logs.size() < maxLogsPerSubnamespace) {
                        URI uri = uriList.get(i);
                        createLogInNamespace(uri, logName, createPromise);
                        return;
                    }
                }
                // All sub namespaces are full
                createSubNamespace().whenComplete(new FutureEventListener<URI>() {
                    @Override
                    public void onSuccess(URI uri) {
                        // the new namespace will be propagated to the namespace cache by the namespace listener
                        // so we don't need to cache it here. we could go ahead to create the stream under this
                        // namespace, as we are using sequential znode. we are mostly the first guy who create
                        // the log under this namespace.
                        createLogInNamespace(uri, logName, createPromise);
                    }

                    @Override
                    public void onFailure(Throwable cause) {
                        createPromise.completeExceptionally(cause);
                    }
                });
            }

            @Override
            public void onFailure(Throwable cause) {
                createPromise.completeExceptionally(cause);
            }
        });
    }

    private String getNamespaceFromZkPath(String zkPath) throws UnexpectedException {
        String[] parts = zkPath.split(SUB_NAMESPACE_PREFIX);
        if (parts.length <= 0) {
            throw new UnexpectedException("Invalid namespace @ " + zkPath);
        }
        return SUB_NAMESPACE_PREFIX + parts[parts.length - 1];
    }

    CompletableFuture<URI> createSubNamespace() {
        final CompletableFuture<URI> promise = new CompletableFuture<URI>();

        final String nsPath = namespace.getPath() + "/" + ZNODE_SUB_NAMESPACES + "/" + SUB_NAMESPACE_PREFIX;
        try {
            zkc.get().create(nsPath, new byte[0], zkc.getDefaultACL(), CreateMode.PERSISTENT_SEQUENTIAL,
                    new AsyncCallback.StringCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, String name) {
                            if (Code.OK.intValue() == rc) {
                                try {
                                    URI newUri = getSubNamespaceURI(getNamespaceFromZkPath(name));
                                    logger.info("Created sub namespace {}", newUri);
                                    promise.complete(newUri);
                                } catch (UnexpectedException ue) {
                                    promise.completeExceptionally(ue);
                                } catch (URISyntaxException e) {
                                    promise.completeExceptionally(
                                            new UnexpectedException("Invalid namespace " + name + " is created."));
                                }
                            } else {
                                promise.completeExceptionally(KeeperException.create(Code.get(rc)));
                            }
                        }
                    }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            promise.completeExceptionally(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            promise.completeExceptionally(e);
        }

        return promise;
    }

    /**
     * Create a log under the namespace. To guarantee there is only one creation happens at time
     * in a federated namespace, we use CAS operation in zookeeper.
     *
     * @param uri
     *          namespace
     * @param logName
     *          name of the log
     * @param createPromise
     *          the promise representing the creation result.
     */
    private void createLogInNamespace(final URI uri,
                                      final String logName,
                                      final CompletableFuture<URI> createPromise) {
        // TODO: rewrite this after we bump to zk 3.5, where we will have asynchronous version of multi
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    createLogInNamespaceSync(uri, logName);
                    createPromise.complete(uri);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    createPromise.completeExceptionally(e);
                } catch (IOException e) {
                    createPromise.completeExceptionally(e);
                } catch (KeeperException.BadVersionException bve) {
                    fetchSubNamespacesAndCreateLog(logName, createPromise);
                } catch (KeeperException e) {
                    createPromise.completeExceptionally(e);
                }
            }
        });
    }

    void createLogInNamespaceSync(URI uri, String logName)
            throws InterruptedException, IOException, KeeperException {
        Transaction txn = zkc.get().transaction();
        // we don't have the zk version yet. set it to 0 instead of -1, to prevent non CAS operation.
        int zkVersion = null == zkSubnamespacesVersion.get() ? 0 : zkSubnamespacesVersion.get();
        txn.setData(zkSubnamespacesPath, uri.getPath().getBytes(UTF_8), zkVersion);
        String logPath = uri.getPath() + "/" + logName;
        txn.create(logPath, new byte[0], zkc.getDefaultACL(), CreateMode.PERSISTENT);
        try {
            txn.commit();
            // if the transaction succeed, the zk version is advanced
            setZkSubnamespacesVersion(zkVersion + 1);
        } catch (KeeperException ke) {
            List<OpResult> opResults = ke.getResults();
            OpResult createResult = opResults.get(1);
            if (createResult instanceof OpResult.ErrorResult) {
                OpResult.ErrorResult errorResult = (OpResult.ErrorResult) createResult;
                if (Code.NODEEXISTS.intValue() == errorResult.getErr()) {
                    throw new LogExistsException("Log " + logName + " already exists");
                }
            }
            OpResult setResult = opResults.get(0);
            if (setResult instanceof OpResult.ErrorResult) {
                OpResult.ErrorResult errorResult = (OpResult.ErrorResult) setResult;
                if (Code.BADVERSION.intValue() == errorResult.getErr()) {
                    throw KeeperException.create(Code.BADVERSION);
                }
            }
            throw new ZKException("ZK exception in creating log " + logName + " in " + uri, ke);
        }
    }

    void setZkSubnamespacesVersion(int zkVersion) {
        Integer oldVersion;
        boolean done = false;
        while (!done) {
            oldVersion = zkSubnamespacesVersion.get();
            if (null == oldVersion) {
                done = zkSubnamespacesVersion.compareAndSet(null, zkVersion);
                continue;
            }
            if (oldVersion < zkVersion) {
                done = zkSubnamespacesVersion.compareAndSet(oldVersion, zkVersion);
                continue;
            } else {
                done = true;
            }
        }
    }

    @Override
    public CompletableFuture<Optional<URI>> getLogLocation(final String logName) {
        if (duplicatedLogFound.get()) {
            return duplicatedLogException(duplicatedLogName.get());
        }
        URI location = log2Locations.get(logName);
        if (null != location) {
            return postStateCheck(FutureUtils.value(Optional.of(location)));
        }
        if (!forceCheckLogExistence) {
            Optional<URI> result = Optional.empty();
            return FutureUtils.value(result);
        }
        return postStateCheck(fetchLogLocation(logName).thenApply((uriOptional) -> {
            if (uriOptional.isPresent()) {
                log2Locations.putIfAbsent(logName, uriOptional.get());
            }
            return uriOptional;
        }));
    }

    private CompletableFuture<Optional<URI>> fetchLogLocation(final String logName) {
        final CompletableFuture<Optional<URI>> fetchPromise = new CompletableFuture<Optional<URI>>();

        Set<URI> uris = subNamespaces.keySet();
        List<CompletableFuture<Optional<URI>>> fetchFutures = Lists.newArrayListWithExpectedSize(uris.size());
        for (URI uri : uris) {
            fetchFutures.add(fetchLogLocation(uri, logName));
        }
        FutureUtils.collect(fetchFutures).whenComplete(new FutureEventListener<List<Optional<URI>>>() {
            @Override
            public void onSuccess(List<Optional<URI>> fetchResults) {
                Optional<URI> result = Optional.empty();
                for (Optional<URI> fetchResult : fetchResults) {
                    if (result.isPresent()) {
                        if (fetchResult.isPresent()) {
                            logger.error("Log {} is found in multiple sub namespaces : {} & {}.",
                                logName, result.get(), fetchResult.get());
                            duplicatedLogName.compareAndSet(null, logName);
                            duplicatedLogFound.set(true);
                            fetchPromise.completeExceptionally(new UnexpectedException("Log " + logName
                                    + " is found in multiple sub namespaces : "
                                    + result.get() + " & " + fetchResult.get()));
                            return;
                        }
                    } else {
                        result = fetchResult;
                    }
                }
                fetchPromise.complete(result);
            }

            @Override
            public void onFailure(Throwable cause) {
                fetchPromise.completeExceptionally(cause);
            }
        });
        return fetchPromise;
    }

    private CompletableFuture<Optional<URI>> fetchLogLocation(final URI uri, String logName) {
        final CompletableFuture<Optional<URI>> fetchPromise = new CompletableFuture<Optional<URI>>();
        final String logRootPath = uri.getPath() + "/" + logName;
        try {
            zkc.get().exists(logRootPath, false, new AsyncCallback.StatCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, Stat stat) {
                    if (Code.OK.intValue() == rc) {
                        fetchPromise.complete(Optional.of(uri));
                    } else if (Code.NONODE.intValue() == rc) {
                        fetchPromise.complete(Optional.<URI>empty());
                    } else {
                        fetchPromise.completeExceptionally(KeeperException.create(Code.get(rc)));
                    }
                }
            }, null);
        } catch (ZooKeeperClient.ZooKeeperConnectionException e) {
            fetchPromise.completeExceptionally(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fetchPromise.completeExceptionally(e);
        }
        return fetchPromise;
    }

    @Override
    public CompletableFuture<Iterator<String>> getLogs(String logNamePrefix) {
        if (!"".equals(logNamePrefix)) {
            return FutureUtils.exception(
                new UnexpectedException("Get logs by prefix is not supported by federated metadata store"));
        }

        if (duplicatedLogFound.get()) {
            return duplicatedLogException(duplicatedLogName.get());
        }
        return postStateCheck(retrieveLogs().thenApply(
            resultList -> getIterator(resultList)));
    }

    private CompletableFuture<List<Set<String>>> retrieveLogs() {
        Collection<SubNamespace> subNss = subNamespaces.values();
        List<CompletableFuture<Set<String>>> logsList = Lists.newArrayListWithExpectedSize(subNss.size());
        for (SubNamespace subNs : subNss) {
            logsList.add(subNs.getLogs());
        }
        return FutureUtils.collect(logsList);
    }

    private Iterator<String> getIterator(List<Set<String>> resultList) {
        List<Iterator<String>> iterList = Lists.newArrayListWithExpectedSize(resultList.size());
        for (Set<String> result : resultList) {
            iterList.add(result.iterator());
        }
        return Iterators.concat(iterList.iterator());
    }

    @Override
    public void registerNamespaceListener(NamespaceListener listener) {
        registerListener(listener);
    }

    @Override
    protected void watchNamespaceChanges() {
        // as the federated namespace already started watching namespace changes,
        // we don't need to do any actions here
    }

    private void notifyOnNamespaceChanges() {
        retrieveLogs().thenAccept(resultList -> {
            for (NamespaceListener listener : listeners) {
                listener.onStreamsChanged(getIterator(resultList));
            }
        });
    }
}
