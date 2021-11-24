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

import static org.apache.bookkeeper.metadata.etcd.EtcdUtils.msResult;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.etcd.jetcd.Lease;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.support.CloseableClient;

import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.bookie.BookieException.MetadataStoreException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.discover.RegistrationManager.RegistrationListener;

/**
 * Register to register a bookie in Etcd.
 */
@Slf4j
class EtcdBookieRegister implements AutoCloseable, Runnable, Supplier<Long> {

    private final Lease leaseClient;
    private final long ttlSeconds;
    private final ScheduledExecutorService executor;
    private RegistrationListener regListener;
    private volatile CompletableFuture<Long> leaseFuture = new CompletableFuture<>();
    private volatile CompletableFuture<Void> keepAliveFuture = new CompletableFuture<>();

    @Getter(AccessLevel.PACKAGE)
    private volatile long leaseId = -0xabcd;
    private volatile CloseableClient kaListener = null;
    private volatile boolean running = true;
    private long nextWaitTimeMs = 200;
    private Future<?> runFuture = null;

    EtcdBookieRegister(Lease leaseClient,
                       long ttlSeconds) {
        this.leaseClient = leaseClient;
        this.ttlSeconds = ttlSeconds;
        this.executor = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("bookie-etcd-keepalive-thread")
                .build());
    }

    public EtcdBookieRegister addRegistrationListener(RegistrationListener regListener) {
        this.regListener = regListener;
        return this;
    }

    long getTtlSeconds() {
        return ttlSeconds;
    }

    public synchronized EtcdBookieRegister start() {
        if (null == runFuture) {
            runFuture = executor.submit(this);
        }
        return this;
    }

    private void newLeaseIfNeeded() throws MetadataStoreException {
        boolean newLeaseNeeded;
        synchronized (this) {
            newLeaseNeeded = !leaseFuture.isDone();
        }
        if (newLeaseNeeded) {
            long leaseId = msResult(leaseClient.grant(ttlSeconds)).getID();
            keepAliveFuture = new CompletableFuture<>();
            if (kaListener != null) {
                synchronized (this) {
                    kaListener.close();
                    kaListener = null;
                }
            }
            this.kaListener = leaseClient.keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
                @Override
                public void onNext(LeaseKeepAliveResponse response) {
                    log.info("KeepAlive response : lease = {}, ttl = {}",
                            response.getID(), response.getTTL());
                }

                @Override
                public void onError(Throwable t) {
                    log.info("KeepAlive renewal failed, leaseId {}", leaseId, t.fillInStackTrace());
                    keepAliveFuture.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    log.info("lease completed! leaseId {}", leaseId);
                    keepAliveFuture.cancel(true);
                }
            });

            this.leaseId = leaseId;
            leaseFuture.complete(leaseId);
            log.info("New lease '{}' is granted.", leaseId);
        }
    }

    private void waitForNewLeaseId() {
        while (running) {
            try {
                newLeaseIfNeeded();
                nextWaitTimeMs = 100L;
            } catch (MetadataStoreException e) {
                log.error("Failed to grant a new lease for leaseId {}", leaseId, e);
                try {
                    TimeUnit.MILLISECONDS.sleep(nextWaitTimeMs);
                    nextWaitTimeMs *= 2;
                    nextWaitTimeMs = Math.min(nextWaitTimeMs, TimeUnit.SECONDS.toMillis(ttlSeconds));
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                    log.warn("Interrupted at backing off granting a new lease for leaseId {}", leaseId);
                }
                continue;
            }
        }
    }

    @Override
    public void run() {
        while (running) {
            waitForNewLeaseId();
            // here we get a lease, keep it alive
            try {
                log.info("Keeping Alive at lease = {}", get());
                keepAliveFuture.get();
                continue;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted at keeping lease '{}' alive", leaseId);
                resetLease();
            } catch (ExecutionException ee) {
                log.warn("Failed to keep alive lease '{}'", leaseId, ee);
                resetLease();
            }
        }
    }

    private void resetLease() {
        synchronized (this) {
            leaseFuture = new CompletableFuture<>();
        }
        if (null != regListener) {
            regListener.onRegistrationExpired();
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (!running) {
                return;
            } else {
                running = false;
            }
            if (null != runFuture) {
                if (runFuture.cancel(true)) {
                    log.info("Successfully interrupted bookie register.");
                }
            }
            keepAliveFuture.cancel(true);
            if (kaListener != null) {
                kaListener.close();
                kaListener = null;
            }
        }
        CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        executor.submit(() -> {
            FutureUtils.complete(closeFuture, (Void) null);
        });
        closeFuture.join();
    }

    @Override
    public Long get() {
        while (true) {
            try {
                return leaseFuture.get(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("Interrupted at getting lease id", e);
                return -1L;
            } catch (ExecutionException e) {
                throw new IllegalArgumentException("Should never reach here");
            } catch (TimeoutException e) {
                continue;
            }

        }
    }

}
