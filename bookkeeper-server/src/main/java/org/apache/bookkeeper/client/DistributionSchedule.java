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

import org.apache.bookkeeper.net.BookieSocketAddress;

import java.util.Map;

/**
 * This interface determins how entries are distributed among bookies.
 *
 * Every entry gets replicated to some number of replicas. The first replica for
 * an entry is given a replicaIndex of 0, and so on. To distribute write load,
 * not all entries go to all bookies. Given an entry-id and replica index, an
 * {@link DistributionSchedule} determines which bookie that replica should go
 * to.
 */

public interface DistributionSchedule {

    /**
     * A write set represents the set of bookies to which
     * a request will be written.
     * The set consists of a list of indices which can be
     * used to lookup the bookie in the ensemble.
     */
    public interface WriteSet {
        /**
         * The number of indexes in the write set.
         */
        public int size();

        /**
         * Whether the set contains the given index.
         */
        public boolean contains(int i);

        /**
         * Get the index at index i.
         */
        public int get(int i);

        /**
         * Set the index at index i.
         * @return the previous value at that index.
         */
        public int set(int i, int index);

        /**
         * Sort the indices
         */
        public void sort();

        /**
         * Index of a specified bookie index.
         * -1 if not found.
         */
        public int indexOf(int index);

        /**
         * If we want a write set to cover all bookies in an ensemble
         * of size X, then all of the index from 0..X must exist in the
         * write set. This method appends those which are missing to the
         * end of the write set.
         */
        public void addMissingIndices(int maxIndex);

        /**
         * Move an index from one position to another,
         * shifting the other indices accordingly.
         */
        public void moveAndShift(int from, int to);

        /**
         * Recycle write set object when not in use.
         */
        public void recycle();

        /**
         * Make a deep copy of this write set.
         */
        public WriteSet copy();
    }

    public static WriteSet NULL_WRITE_SET = new WriteSet() {
            @Override
            public int size() { return 0; }
            @Override
            public boolean contains(int i) { return false; }
            @Override
            public int get(int i) {
                throw new ArrayIndexOutOfBoundsException();
            }
            @Override
            public int set(int i, int index) {
                throw new ArrayIndexOutOfBoundsException();
            }
            @Override
            public void sort() {}
            @Override
            public int indexOf(int index) { return -1; }
            @Override
            public void addMissingIndices(int maxIndex) {
                throw new ArrayIndexOutOfBoundsException();
            }
            @Override
            public void moveAndShift(int from, int to) {
                throw new ArrayIndexOutOfBoundsException();
            }
            @Override
            public void recycle() {}
            @Override
            public WriteSet copy() { return this; }
        };

    /**
     * return the set of bookie indices to send the message to
     */
    public WriteSet getWriteSet(long entryId);


    /**
     * An ack set represents the set of bookies from which
     * a response must be received so that an entry can be
     * considered to be replicated on a quorum.
     */
    public interface AckSet {
        /**
         * Add a bookie response and check if quorum has been met
         * @return true if quorum has been met, false otherwise
         */
        public boolean completeBookieAndCheck(int bookieIndexHeardFrom);

        /**
         * Received failure response from a bookie and check if ack quorum
         * will be broken.
         *
         * @param bookieIndexHeardFrom
         *          bookie index that failed.
         * @param address
         *          bookie address
         * @return true if ack quorum is broken, false otherwise.
         */
        public boolean failBookieAndCheck(int bookieIndexHeardFrom, BookieSocketAddress address);

        /**
         * Return the list of bookies that already failed.
         *
         * @return the list of bookies that already failed.
         */
        public Map<Integer, BookieSocketAddress> getFailedBookies();

        /**
         * Invalidate a previous bookie response.
         * Used for reissuing write requests.
         */
        public boolean removeBookieAndCheck(int bookie);

        /**
         * Recycle this ack set when not used anymore
         */
        public void recycle();
    }

    /**
     * Returns an ackset object, responses should be checked against this
     */
    public AckSet getAckSet();


    /**
     * Interface to keep track of which bookies in an ensemble, an action
     * has been performed for.
     */
    public interface QuorumCoverageSet {
        /**
         * Add a bookie to the result set
         *
         * @param bookieIndexHeardFrom Bookie we've just heard from
         */
        void addBookie(int bookieIndexHeardFrom, int rc);

        /**
         * check if all quorum in the set have had the action performed for it.
         *
         * @return whether all quorums have been covered
         */
        boolean checkCovered();
    }

    public QuorumCoverageSet getCoverageSet();

    /**
     * Whether entry presents on given bookie index
     *
     * @param entryId
     *            - entryId to check the presence on given bookie index
     * @param bookieIndex
     *            - bookie index on which it need to check the possible presence
     *            of the entry
     * @return true if it has entry otherwise false.
     */
    public boolean hasEntry(long entryId, int bookieIndex);
}
