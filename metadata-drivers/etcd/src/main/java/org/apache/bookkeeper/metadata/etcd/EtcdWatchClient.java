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

package org.apache.bookkeeper.metadata.etcd;

import static com.coreos.jetcd.common.exception.EtcdExceptionFactory.newClosedWatchClientException;
import static com.coreos.jetcd.common.exception.EtcdExceptionFactory.newEtcdException;
import static com.coreos.jetcd.common.exception.EtcdExceptionFactory.toEtcdException;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.api.WatchCancelRequest;
import com.coreos.jetcd.api.WatchCreateRequest;
import com.coreos.jetcd.api.WatchGrpc;
import com.coreos.jetcd.api.WatchRequest;
import com.coreos.jetcd.api.WatchResponse;
import com.coreos.jetcd.common.exception.ErrorCode;
import com.coreos.jetcd.common.exception.EtcdException;
import com.coreos.jetcd.common.exception.EtcdExceptionFactory;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.internal.impl.EtcdConnectionManager;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchResponseWithError;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashMap;
import org.apache.bookkeeper.util.collections.ConcurrentLongHashSet;

/**
 * An async watch implementation.
 */
@Slf4j
public class EtcdWatchClient implements AutoCloseable {

    private final EtcdConnectionManager connMgr;
    private final WatchGrpc.WatchStub stub;
    private volatile StreamObserver<WatchRequest> grpcWatchStreamObserver;
    // watchers stores a mapping between watchID -> EtcdWatcher.
    private final ConcurrentLongHashMap<EtcdWatcher> watchers =
        new ConcurrentLongHashMap<>();
    private final LinkedList<EtcdWatcher> pendingWatchers = new LinkedList<>();
    private final ConcurrentLongHashSet cancelSet = new ConcurrentLongHashSet();

    // scheduler
    private final OrderedScheduler scheduler;
    private final ScheduledExecutorService watchExecutor;

    // close state
    private CompletableFuture<Void> closeFuture = null;

    public EtcdWatchClient(Client client) {
        this.connMgr = new EtcdConnectionManager(client);
        this.stub = connMgr.newWatchStub();
        this.scheduler = OrderedScheduler.newSchedulerBuilder()
            .name("etcd-watcher-scheduler")
            .numThreads(Runtime.getRuntime().availableProcessors())
            .build();
        this.watchExecutor = this.scheduler.chooseThread();
    }

    public synchronized boolean isClosed() {
        return closeFuture != null;
    }

    public CompletableFuture<EtcdWatcher> watch(ByteSequence key,
                                                BiConsumer<com.coreos.jetcd.watch.WatchResponse, Throwable> consumer) {
        return watch(key, WatchOption.DEFAULT, consumer);
    }

    public CompletableFuture<EtcdWatcher> watch(ByteSequence key,
                                                WatchOption watchOption,
                                                BiConsumer<com.coreos.jetcd.watch.WatchResponse, Throwable> consumer) {
        return CompletableFuture.supplyAsync(() -> {
            if (isClosed()) {
                throw EtcdExceptionFactory.newClosedWatchClientException();
            }

            EtcdWatcher watcher = new EtcdWatcher(key, watchOption, scheduler.chooseThread(), this);
            watcher.addConsumer(consumer);
            pendingWatchers.add(watcher);
            if (pendingWatchers.size() == 1) {
                WatchRequest request = toWatchCreateRequest(watcher);
                getGrpcWatchStreamObserver().onNext(request);
            }
            return watcher;
        }, watchExecutor);
    }

    // notifies all watchers about a exception. it doesn't close watchers.
    // it is the responsibility of user to close watchers.
    private void notifyWatchers(EtcdException e) {
        WatchResponseWithError wre = new WatchResponseWithError(e);
        this.pendingWatchers.forEach(watcher -> watcher.notifyWatchResponse(wre));
        this.pendingWatchers.clear();
        this.watchers.values().forEach(watcher -> watcher.notifyWatchResponse(wre));
        this.watchers.clear();
    }

    public CompletableFuture<Void> unwatch(EtcdWatcher watcher) {
        return CompletableFuture.runAsync(() -> cancelWatcher(watcher.getWatchID()), watchExecutor);
    }

    private void cancelWatcher(long watchID) {
        if (isClosed()) {
            return;
        }

        if (cancelSet.contains(watchID)) {
            return;
        }

        watchers.remove(watchID);
        cancelSet.add(watchID);

        WatchCancelRequest watchCancelRequest = WatchCancelRequest.newBuilder()
            .setWatchId(watchID)
            .build();
        WatchRequest cancelRequest = WatchRequest.newBuilder()
            .setCancelRequest(watchCancelRequest)
            .build();
        getGrpcWatchStreamObserver().onNext(cancelRequest);
    }

    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> future;
        synchronized (this) {
            if (null == closeFuture) {
                log.info("Closing watch client");
                closeFuture = CompletableFuture.runAsync(() -> {
                    notifyWatchers(newClosedWatchClientException());
                    closeGrpcWatchStreamObserver();
                }, watchExecutor);
            }
            future = closeFuture;
        }
        return future.whenComplete((ignored, cause) -> {
            this.scheduler.shutdown();
        });
    }

    @Override
    public void close() {
        try {
            FutureUtils.result(closeAsync());
        } catch (Exception e) {
            log.warn("Encountered exceptions on closing watch client", e);
        }
        this.scheduler.forceShutdown(10, TimeUnit.SECONDS);
    }

    private StreamObserver<WatchResponse> createWatchStreamObserver() {
        return new StreamObserver<WatchResponse>() {
            @Override
            public void onNext(WatchResponse watchResponse) {
                if (isClosed()) {
                    return;
                }
                watchExecutor.submit(() -> processWatchResponse(watchResponse));
            }

            @Override
            public void onError(Throwable t) {
                if (isClosed()) {
                    return;
                }
                watchExecutor.submit(() -> processError(t));
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private void processWatchResponse(WatchResponse watchResponse) {
        // prevents grpc on sending watchResponse to a closed watch client.
        if (isClosed()) {
            return;
        }

        if (watchResponse.getCreated()) {
            processCreate(watchResponse);
        } else if (watchResponse.getCanceled()) {
            processCanceled(watchResponse);
        } else {
            processEvents(watchResponse);
        }
    }

    private void processError(Throwable t) {
        // prevents grpc on sending error to a closed watch client.
        if (this.isClosed()) {
            return;
        }

        Status status = Status.fromThrowable(t);
        if (this.isHaltError(status) || this.isNoLeaderError(status)) {
            this.notifyWatchers(toEtcdException(status));
            this.closeGrpcWatchStreamObserver();
            this.cancelSet.clear();
            return;
        }
        // resume with a delay; avoiding immediate retry on a long connection downtime.
        scheduler.schedule(this::resume, 500, TimeUnit.MILLISECONDS);
    }

    private void resume() {
        this.closeGrpcWatchStreamObserver();
        this.cancelSet.clear();
        this.resumeWatchers();
    }

    private synchronized StreamObserver<WatchRequest> getGrpcWatchStreamObserver() {
        if (this.grpcWatchStreamObserver == null) {
            this.grpcWatchStreamObserver = this.stub.watch(this.createWatchStreamObserver());
        }
        return this.grpcWatchStreamObserver;
    }

    // closeGrpcWatchStreamObserver closes the underlying grpc watch stream.
    private void closeGrpcWatchStreamObserver() {
        if (this.grpcWatchStreamObserver == null) {
            return;
        }
        this.grpcWatchStreamObserver.onCompleted();
        this.grpcWatchStreamObserver = null;
    }

    private void processCreate(WatchResponse response) {
        EtcdWatcher watcher = this.pendingWatchers.poll();

        this.sendNextWatchCreateRequest();

        if (watcher == null) {
            // shouldn't happen
            // may happen due to duplicate watch create responses.
            log.warn("Watch client receives watch create response but find no corresponding watcher");
            return;
        }

        if (watcher.isClosed()) {
            return;
        }

        if (response.getWatchId() == -1) {
            watcher.notifyWatchResponse(new WatchResponseWithError(
                newEtcdException(ErrorCode.INTERNAL, "etcd server failed to create watch id")));
            return;
        }

        if (watcher.getRevision() == 0) {
            watcher.setRevision(response.getHeader().getRevision());
        }

        watcher.setWatchID(response.getWatchId());
        this.watchers.put(watcher.getWatchID(), watcher);
    }

    /**
     * chooses the next resuming watcher to register with the grpc stream.
     */
    private Optional<WatchRequest> nextResume() {
        EtcdWatcher pendingWatcher = this.pendingWatchers.peek();
        if (pendingWatcher != null) {
            return Optional.of(this.toWatchCreateRequest(pendingWatcher));
        }
        return Optional.empty();
    }

    private void sendNextWatchCreateRequest() {
        this.nextResume().ifPresent(
            (nextWatchRequest -> this.getGrpcWatchStreamObserver().onNext(nextWatchRequest)));
    }

    private void processEvents(WatchResponse response) {
        EtcdWatcher watcher = this.watchers.get(response.getWatchId());
        if (watcher == null) {
            // cancel server side watcher.
            this.cancelWatcher(response.getWatchId());
            return;
        }

        if (response.getCompactRevision() != 0) {
            watcher.notifyWatchResponse(new WatchResponseWithError(
                EtcdExceptionFactory
                    .newCompactedException(response.getCompactRevision())));
            return;
        }

        if (response.getEventsCount() == 0) {
            watcher.setRevision(response.getHeader().getRevision());
            return;
        }

        watcher.notifyWatchResponse(new WatchResponseWithError(response));
        watcher.setRevision(
            response
                .getEvents(response.getEventsCount() - 1)
                .getKv().getModRevision() + 1);
    }

    private void resumeWatchers() {
        this.watchers.values().forEach(watcher -> {
            if (watcher.isClosed()) {
                return;
            }
            watcher.setWatchID(-1);
            this.pendingWatchers.add(watcher);
        });

        this.watchers.clear();

        this.sendNextWatchCreateRequest();
    }

    private void processCanceled(WatchResponse response) {
        EtcdWatcher watcher = this.watchers.get(response.getWatchId());
        this.cancelSet.remove(response.getWatchId());
        if (watcher == null) {
            return;
        }
        String reason = response.getCancelReason();
        if (Strings.isNullOrEmpty(reason)) {
            watcher.notifyWatchResponse(new WatchResponseWithError(newEtcdException(
                ErrorCode.OUT_OF_RANGE,
                "etcdserver: mvcc: required revision is a future revision"))
            );

        } else {
            watcher.notifyWatchResponse(
                new WatchResponseWithError(newEtcdException(ErrorCode.FAILED_PRECONDITION, reason)));
        }
    }

    private static boolean isNoLeaderError(Status status) {
        return status.getCode() == Code.UNAVAILABLE
            && "etcdserver: no leader".equals(status.getDescription());
    }

    private static boolean isHaltError(Status status) {
        // Unavailable codes mean the system will be right back.
        // (e.g., can't connect, lost leader)
        // Treat Internal codes as if something failed, leaving the
        // system in an inconsistent state, but retrying could make progress.
        // (e.g., failed in middle of send, corrupted frame)
        return status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.INTERNAL;
    }

    private static WatchRequest toWatchCreateRequest(EtcdWatcher watcher) {
        ByteString key = UnsafeByteOperations.unsafeWrap(watcher.getKey().getBytes());
        WatchOption option = watcher.getWatchOption();
        WatchCreateRequest.Builder builder = WatchCreateRequest.newBuilder()
            .setKey(key)
            .setPrevKv(option.isPrevKV())
            .setProgressNotify(option.isProgressNotify())
            .setStartRevision(watcher.getRevision());

        option.getEndKey()
            .ifPresent(endKey -> builder.setRangeEnd(UnsafeByteOperations.unsafeWrap(endKey.getBytes())));

        if (option.isNoDelete()) {
            builder.addFilters(WatchCreateRequest.FilterType.NODELETE);
        }

        if (option.isNoPut()) {
            builder.addFilters(WatchCreateRequest.FilterType.NOPUT);
        }

        return WatchRequest.newBuilder().setCreateRequest(builder).build();
    }


}
