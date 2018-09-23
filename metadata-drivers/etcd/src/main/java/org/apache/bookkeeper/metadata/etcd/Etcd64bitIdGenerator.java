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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.bookkeeper.metadata.etcd.EtcdConstants.EMPTY_BS;

import com.coreos.jetcd.KV;
import com.coreos.jetcd.Txn;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.kv.GetResponse;
import com.coreos.jetcd.op.Cmp;
import com.coreos.jetcd.op.Cmp.Op;
import com.coreos.jetcd.op.CmpTarget;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.PutOption;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException.Code;
import org.apache.bookkeeper.meta.LedgerIdGenerator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;

/**
 * Generate 64-bit ledger ids from a bucket.
 *
 * <p>The most significant 8 bits is used as bucket id. The remaining 56 bits are
 * used as the id generated per bucket.
 */
@Slf4j
class Etcd64bitIdGenerator implements LedgerIdGenerator {

    static final long MAX_ID_PER_BUCKET = 0x00ffffffffffffffL;
    static final long BUCKET_ID_MASK    = 0xff00000000000000L;
    static final int BUCKET_ID_SHIFT    = 56;
    static final int NUM_BUCKETS        = 0x80;

    static int getBucketId(long lid) {
        return (int) ((lid & BUCKET_ID_MASK) >>> BUCKET_ID_SHIFT);
    }

    static long getIdInBucket(long lid) {
        return lid & MAX_ID_PER_BUCKET;
    }

    private static final AtomicIntegerFieldUpdater<Etcd64bitIdGenerator> nextBucketIdUpdater =
        AtomicIntegerFieldUpdater.newUpdater(Etcd64bitIdGenerator.class, "nextBucketId");

    private final String scope;
    private final KV kvClient;
    private volatile int nextBucketId;

    Etcd64bitIdGenerator(KV kvClient, String scope) {
        this.kvClient = kvClient;
        this.scope = scope;
        this.nextBucketId = ThreadLocalRandom.current().nextInt(NUM_BUCKETS);
    }

    int nextBucketId() {
        while (true) {
            int bucketId = nextBucketIdUpdater.incrementAndGet(this);
            if (bucketId >= NUM_BUCKETS) {
                if (nextBucketIdUpdater.compareAndSet(this, bucketId, 0)) {
                    bucketId = 0;
                } else {
                    // someone has been updated bucketId, try it again.
                    continue;
                }
            }
            return bucketId;
        }
    }

    @Override
    public void generateLedgerId(GenericCallback<Long> cb) {
        int bucketId = nextBucketId();
        checkArgument(bucketId >= 0 && bucketId < NUM_BUCKETS,
            "Invalid bucket id : " + bucketId);

        ByteSequence bucketKey = ByteSequence.fromString(EtcdUtils.getBucketPath(scope, bucketId));
        Txn txn = kvClient.txn()
            .If(new Cmp(bucketKey, Op.GREATER, CmpTarget.createRevision(0)))
            .Then(
                com.coreos.jetcd.op.Op.put(bucketKey, EMPTY_BS, PutOption.DEFAULT),
                com.coreos.jetcd.op.Op.get(bucketKey, GetOption.DEFAULT)
            )
            .Else(
                com.coreos.jetcd.op.Op.put(bucketKey, EMPTY_BS, PutOption.DEFAULT),
                com.coreos.jetcd.op.Op.get(bucketKey, GetOption.DEFAULT)
            );
        txn.commit()
            .thenAccept(txnResponse -> {
                if (txnResponse.getGetResponses().size() <= 0) {
                    cb.operationComplete(Code.UnexpectedConditionException, null);
                } else {
                    GetResponse resp = txnResponse.getGetResponses().get(0);
                    if (resp.getCount() > 0) {
                        KeyValue kv = resp.getKvs().get(0);
                        if (kv.getVersion() > MAX_ID_PER_BUCKET) {
                            log.warn("Etcd bucket '{}' is overflowed", bucketKey.toStringUtf8());
                            // the bucket is overflowed, moved to next bucket.
                            generateLedgerId(cb);
                        } else {
                            long version = kv.getVersion();
                            long lid = ((((long) bucketId) << BUCKET_ID_SHIFT) & BUCKET_ID_MASK)
                                | (version & MAX_ID_PER_BUCKET);
                            cb.operationComplete(Code.OK, lid);
                        }
                    } else {
                        cb.operationComplete(Code.UnexpectedConditionException, null);
                    }
                }
            })
            .exceptionally(cause -> {
                cb.operationComplete(Code.MetaStoreException, null);
                return null;
            });
    }

    @Override
    public void close() {
        // no-op
    }
}
