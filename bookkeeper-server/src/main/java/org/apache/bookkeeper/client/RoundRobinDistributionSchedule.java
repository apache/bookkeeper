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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A specific {@link DistributionSchedule} that places entries in round-robin
 * fashion. For ensemble size 3, and quorum size 2, Entry 0 goes to bookie 0 and
 * 1, entry 1 goes to bookie 1 and 2, and entry 2 goes to bookie 2 and 0, and so
 * on.
 *
 */
public class RoundRobinDistributionSchedule implements DistributionSchedule {
    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinDistributionSchedule.class);
    private final int writeQuorumSize;
    private final int ackQuorumSize;
    private final int ensembleSize;

    public RoundRobinDistributionSchedule(int writeQuorumSize, int ackQuorumSize, int ensembleSize) {
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;
        this.ensembleSize = ensembleSize;
    }

    @Override
    public WriteSet getWriteSet(long entryId) {
        return WriteSetImpl.create(ensembleSize, writeQuorumSize, entryId);
    }

    @Override
    public WriteSet getEnsembleSet(long entryId) {
        // for long poll reads and force ledger , we are trying all the bookies in the ensemble
        // so we create a `WriteSet` with `writeQuorumSize == ensembleSize`.
        return WriteSetImpl.create(ensembleSize, ensembleSize /* writeQuorumSize */, entryId);
    }

    @VisibleForTesting
    static WriteSet writeSetFromValues(Integer... values) {
        WriteSetImpl writeSet = WriteSetImpl.create(0, 0, 0);
        writeSet.setSize(values.length);
        for (int i = 0; i < values.length; i++) {
            writeSet.set(i, values[i]);
        }
        return writeSet;
    }

    private static class WriteSetImpl implements WriteSet {
        int[] array = null;
        int size;

        private final Handle<WriteSetImpl> recyclerHandle;
        private static final Recycler<WriteSetImpl> RECYCLER = new Recycler<WriteSetImpl>() {
                    @Override
                    protected WriteSetImpl newObject(
                            Recycler.Handle<WriteSetImpl> handle) {
                        return new WriteSetImpl(handle);
                    }
                };

        private WriteSetImpl(Handle<WriteSetImpl> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static WriteSetImpl create(int ensembleSize,
                                   int writeQuorumSize,
                                   long entryId) {
            WriteSetImpl writeSet = RECYCLER.get();
            writeSet.reset(ensembleSize, writeQuorumSize, entryId);
            return writeSet;
        }

        private void reset(int ensembleSize, int writeQuorumSize,
                           long entryId) {
            setSize(writeQuorumSize);
            for (int w = 0; w < writeQuorumSize; w++) {
                set(w, (int) ((entryId + w) % ensembleSize));
            }
        }

        private void setSize(int newSize) {
            if (array == null) {
                array = new int[newSize];
            } else if (newSize > array.length) {
                int[] newArray = new int[newSize];
                System.arraycopy(array, 0,
                                 newArray, 0, array.length);
                array = newArray;
            }
            size = newSize;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public boolean contains(int i) {
            return indexOf(i) != -1;
        }

        @Override
        public int get(int i) {
            checkBounds(i);
            return array[i];
        }

        @Override
        public int set(int i, int index) {
            checkBounds(i);
            int oldVal = array[i];
            array[i] = index;
            return oldVal;
        }

        @Override
        public void sort() {
            Arrays.sort(array, 0, size);
        }

        @Override
        public int indexOf(int index) {
            for (int j = 0; j < size; j++) {
                if (array[j] == index) {
                    return j;
                }
            }
            return -1;
        }

        @Override
        public void addMissingIndices(int maxIndex) {
            if (size < maxIndex) {
                int oldSize = size;
                setSize(maxIndex);
                for (int i = 0, j = oldSize;
                    i < maxIndex && j < maxIndex; i++) {
                    if (!contains(i)) {
                        set(j, i);
                        j++;
                    }
                }
            }
        }

        @Override
        public void moveAndShift(int from, int to) {
            checkBounds(from);
            checkBounds(to);
            if (from > to) {
                int tmp = array[from];
                for (int i = from; i > to; i--) {
                    array[i] = array[i - 1];
                }
                array[to] = tmp;
            } else if (from < to) {
                int tmp = array[from];
                for (int i = from; i < to; i++) {
                    array[i] = array[i + 1];
                }
                array[to] = tmp;
            }
        }

        @Override
        public void recycle() {
            recyclerHandle.recycle(this);
        }

        @Override
        public WriteSet copy() {
            WriteSetImpl copy = RECYCLER.get();
            copy.setSize(size);
            for (int i = 0; i < size; i++) {
                copy.set(i, array[i]);
            }
            return copy;
        }

        @Override
        public int hashCode() {
            int sum = 0;
            for (int i = 0; i < size; i++) {
                sum += sum * 31 + i;
            }
            return sum;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof WriteSetImpl) {
                WriteSetImpl o = (WriteSetImpl) other;
                if (o.size() != size()) {
                    return false;
                }
                for (int i = 0; i < size(); i++) {
                    if (o.get(i) != get(i)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder("WriteSet[");
            int i = 0;
            for (; i < size() - 1; i++) {
                b.append(get(i)).append(",");
            }
            b.append(get(i)).append("]");
            return b.toString();
        }

        private void checkBounds(int i) {
            if (i < 0 || i > size) {
                throw new IndexOutOfBoundsException(
                        "Index " + i + " out of bounds, array size = " + size);
            }
        }
    }

    @Override
    public AckSet getAckSet() {
        return AckSetImpl.create(ensembleSize, writeQuorumSize, ackQuorumSize);
    }

    @Override
    public AckSet getEnsembleAckSet() {
        return AckSetImpl.create(ensembleSize, ensembleSize, ensembleSize);
    }

    private static class AckSetImpl implements AckSet {
        private int writeQuorumSize;
        private int ackQuorumSize;
        private final BitSet ackSet = new BitSet();
        // grows on reset()
        private BookieSocketAddress[] failureMap = new BookieSocketAddress[0];

        private final Handle<AckSetImpl> recyclerHandle;
        private static final Recycler<AckSetImpl> RECYCLER = new Recycler<AckSetImpl>() {
            @Override
            protected AckSetImpl newObject(Recycler.Handle<AckSetImpl> handle) {
                return new AckSetImpl(handle);
            }
        };

        private AckSetImpl(Handle<AckSetImpl> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static AckSetImpl create(int ensembleSize,
                                 int writeQuorumSize,
                                 int ackQuorumSize) {
            AckSetImpl ackSet = RECYCLER.get();
            ackSet.reset(ensembleSize, writeQuorumSize, ackQuorumSize);
            return ackSet;
        }

        private void reset(int ensembleSize,
                           int writeQuorumSize,
                           int ackQuorumSize) {
            this.ackQuorumSize = ackQuorumSize;
            this.writeQuorumSize = writeQuorumSize;
            ackSet.clear();
            if (failureMap.length < ensembleSize) {
                failureMap = new BookieSocketAddress[ensembleSize];
            }
            Arrays.fill(failureMap, null);
        }

        @Override
        public boolean completeBookieAndCheck(int bookieIndexHeardFrom) {
            failureMap[bookieIndexHeardFrom] = null;
            ackSet.set(bookieIndexHeardFrom);
            return ackSet.cardinality() >= ackQuorumSize;
        }

        @Override
        public boolean failBookieAndCheck(int bookieIndexHeardFrom,
                                          BookieSocketAddress address) {
            ackSet.clear(bookieIndexHeardFrom);
            failureMap[bookieIndexHeardFrom] = address;
            return failed() > (writeQuorumSize - ackQuorumSize);
        }

        @Override
        public Map<Integer, BookieSocketAddress> getFailedBookies() {
            ImmutableMap.Builder<Integer, BookieSocketAddress> builder = new ImmutableMap.Builder<>();
            for (int i = 0; i < failureMap.length; i++) {
                if (failureMap[i] != null) {
                    builder.put(i, failureMap[i]);
                }
            }
            return builder.build();
        }

        @Override
        public boolean removeBookieAndCheck(int bookie) {
            ackSet.clear(bookie);
            failureMap[bookie] = null;
            return ackSet.cardinality() >= ackQuorumSize;
        }

        @Override
        public void recycle() {
            recyclerHandle.recycle(this);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("ackQuorumSize", ackQuorumSize)
                .add("ackSet", ackSet)
                .add("failureMap", failureMap).toString();
        }

        private int failed() {
            int count = 0;
            for (int i = 0; i < failureMap.length; i++) {
                if (failureMap[i] != null) {
                    count++;
                }
            }
            return count;
        }
    }

    private class RRQuorumCoverageSet implements QuorumCoverageSet {
        private final int[] covered = new int[ensembleSize];

        private RRQuorumCoverageSet() {
            for (int i = 0; i < covered.length; i++) {
                covered[i] = BKException.Code.UNINITIALIZED;
            }
        }

        @Override
        public synchronized void addBookie(int bookieIndexHeardFrom, int rc) {
            covered[bookieIndexHeardFrom] = rc;
        }

        @Override
        public synchronized boolean checkCovered() {
            // now check if there are any write quorums, with |ackQuorum| nodes available
            for (int i = 0; i < ensembleSize; i++) {
                /* Nodes which have either responded with an error other than NoSuch{Entry,Ledger},
                   or have not responded at all. We cannot know if these nodes ever accepted a entry. */
                int nodesUnknown = 0;

                for (int j = 0; j < writeQuorumSize; j++) {
                    int nodeIndex = (i + j) % ensembleSize;
                    if (covered[nodeIndex] != BKException.Code.OK
                        && covered[nodeIndex] != BKException.Code.NoSuchEntryException
                        && covered[nodeIndex] != BKException.Code.NoSuchLedgerExistsException) {
                        nodesUnknown++;
                    }
                }

                /* If nodesUnknown is greater than the ack quorum size, then
                   it is possible those two unknown nodes accepted an entry which
                   we do not know about */
                if (nodesUnknown >= ackQuorumSize) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            buffer.append("QuorumCoverage(e:").append(ensembleSize)
                .append(",w:").append(writeQuorumSize)
                .append(",a:").append(ackQuorumSize)
                .append(") = [");
            int i = 0;
            for (; i < covered.length - 1; i++) {
                buffer.append(covered[i]).append(", ");
            }
            buffer.append(covered[i]).append("]");
            return buffer.toString();
        }
    }

    @Override
    public QuorumCoverageSet getCoverageSet() {
        return new RRQuorumCoverageSet();
    }

    @Override
    public boolean hasEntry(long entryId, int bookieIndex) {
        WriteSet w = getWriteSet(entryId);
        try {
            return w.contains(bookieIndex);
        } finally {
            w.recycle();
        }
    }

    @Override
    public BitSet getEntriesStripedToTheBookie(int bookieIndex, long startEntryId, long lastEntryId) {
        if ((startEntryId < 0) || (lastEntryId < 0) || (bookieIndex < 0) || (bookieIndex >= ensembleSize)
                || (lastEntryId < startEntryId)) {
            LOG.error(
                    "Illegal arguments for getEntriesStripedToTheBookie, bookieIndex : {},"
                            + " ensembleSize : {}, startEntryId : {}, lastEntryId : {}",
                    bookieIndex, ensembleSize, startEntryId, lastEntryId);
            throw new IllegalArgumentException("Illegal arguments for getEntriesStripedToTheBookie");
        }
        BitSet entriesStripedToTheBookie = new BitSet((int) (lastEntryId - startEntryId + 1));
        for (long entryId = startEntryId; entryId <= lastEntryId; entryId++) {
            int modValOfFirstReplica = (int) (entryId % ensembleSize);
            int modValOfLastReplica = (int) ((entryId + writeQuorumSize - 1) % ensembleSize);
            if (modValOfLastReplica >= modValOfFirstReplica) {
                if ((bookieIndex >= modValOfFirstReplica) && (bookieIndex <= modValOfLastReplica)) {
                    entriesStripedToTheBookie.set((int) (entryId - startEntryId));
                }
            } else {
                if ((bookieIndex >= modValOfFirstReplica) || (bookieIndex <= modValOfLastReplica)) {
                    entriesStripedToTheBookie.set((int) (entryId - startEntryId));
                }
            }
        }
        return entriesStripedToTheBookie;
    }
}
