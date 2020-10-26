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
package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.PrimitiveIterator;
import java.util.TreeMap;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;

/**
 * Ordered collection of SequenceGroups will represent entries of the ledger
 * residing in a bookie.
 *
 * <p>In the byte array representation of AvailabilityOfEntriesOfLedger, for the
 * sake of future extensibility it would be helpful to have reserved space for
 * header at the beginning. So the first 64 bytes will be used for header, with
 * the first four bytes specifying the int version number, next four bytes
 * specifying the number of sequencegroups for now and the rest of the bytes in
 * the reserved space will be 0's. The encoded format will be represented after
 * the first 64 bytes. The ordered collection of SequenceGroups will be appended
 * sequentially to this byte array, with each SequenceGroup taking 24 bytes.
 */
public class AvailabilityOfEntriesOfLedger {
    public static final long INVALID_ENTRYID = -1;
    public static final AvailabilityOfEntriesOfLedger EMPTY_AVAILABILITYOFENTRIESOFLEDGER;
    static {
        long[] tmpArray = {};
        EMPTY_AVAILABILITYOFENTRIESOFLEDGER = new AvailabilityOfEntriesOfLedger(Arrays.stream(tmpArray).iterator());
    }

    /*
     *
     * Nomenclature:
     *
     * - Continuous entries are grouped as a ’Sequence’. - Number of continuous
     * entries in a ‘Sequence’ is called ‘sequenceSize’. - Gap between
     * Consecutive sequences is called ‘sequencePeriod’. - Consecutive sequences
     * with same sequenceSize and same sequencePeriod in between consecutive
     * sequences are grouped as a SequenceGroup. - ‘firstSequenceStart’ is the
     * first entry in the first sequence of the SequenceGroup. -
     * ‘lastSequenceStart’ is the first entry in the last sequence of the
     * SequenceGroup.
     *
     * To represent a SequenceGroup, two long values and two int values are
     * needed, so each SequenceGroup can be represented with (2 * 8 + 2 * 4 = 24
     * bytes).
     */
    private static class SequenceGroup {
        private static final int SEQUENCEGROUP_BYTES = 2 * Long.BYTES + 2 * Integer.BYTES;
        private final long firstSequenceStart;
        private final int sequenceSize;
        private long lastSequenceStart = INVALID_ENTRYID;
        private int sequencePeriod;
        private boolean isSequenceGroupClosed = false;
        private long numOfEntriesInSequenceGroup = 0;

        private SequenceGroup(long firstSequenceStart, int sequenceSize) {
            this.firstSequenceStart = firstSequenceStart;
            this.lastSequenceStart = firstSequenceStart;
            this.sequenceSize = sequenceSize;
            this.sequencePeriod = 0;
        }

        private SequenceGroup(byte[] serializedSequenceGroup) {
            ByteBuffer buffer = ByteBuffer.wrap(serializedSequenceGroup);
            firstSequenceStart = buffer.getLong();
            lastSequenceStart = buffer.getLong();
            sequenceSize = buffer.getInt();
            sequencePeriod = buffer.getInt();
            setSequenceGroupClosed();
        }

        private boolean isSequenceGroupClosed() {
            return isSequenceGroupClosed;
        }

        private void setSequenceGroupClosed() {
            this.isSequenceGroupClosed = true;
            numOfEntriesInSequenceGroup = (lastSequenceStart - firstSequenceStart) == 0 ? sequenceSize
                    : (((lastSequenceStart - firstSequenceStart) / sequencePeriod) + 1) * sequenceSize;
        }

        private long getNumOfEntriesInSequenceGroup() {
            if (!isSequenceGroupClosed()) {
                throw new IllegalStateException(
                        "SequenceGroup is not yet closed, it is illegal to call getNumOfEntriesInSequenceGroup");
            }
            return numOfEntriesInSequenceGroup;
        }

        private long getLastSequenceStart() {
            return lastSequenceStart;
        }

        private long getLastEntryInSequenceGroup() {
            return lastSequenceStart + sequenceSize;
        }

        private void setLastSequenceStart(long lastSequenceStart) {
            this.lastSequenceStart = lastSequenceStart;
        }

        private int getSequencePeriod() {
            return sequencePeriod;
        }

        private void setSequencePeriod(int sequencePeriod) {
            this.sequencePeriod = sequencePeriod;
        }

        private long getFirstSequenceStart() {
            return firstSequenceStart;
        }

        private void serializeSequenceGroup(byte[] byteArrayForSerialization) {
            if (!isSequenceGroupClosed()) {
                throw new IllegalStateException(
                        "SequenceGroup is not yet closed, it is illegal to call serializeSequenceGroup");
            }
            ByteBuffer buffer = ByteBuffer.wrap(byteArrayForSerialization);
            buffer.putLong(firstSequenceStart);
            buffer.putLong(lastSequenceStart);
            buffer.putInt(sequenceSize);
            buffer.putInt(sequencePeriod);
        }

        private boolean isEntryAvailable(long entryId) {
            if (!isSequenceGroupClosed()) {
                throw new IllegalStateException(
                        "SequenceGroup is not yet closed, it is illegal to call isEntryAvailable");
            }

            if ((entryId >= firstSequenceStart) && (entryId <= (lastSequenceStart + sequenceSize))) {
                if (sequencePeriod == 0) {
                    return ((entryId - firstSequenceStart) < sequenceSize);
                } else {
                    return (((entryId - firstSequenceStart) % sequencePeriod) < sequenceSize);
                }
            } else {
                return false;
            }
        }
    }

    public static final int HEADER_SIZE = 64;
    public static final int V0 = 0;
    // current version of AvailabilityOfEntriesOfLedger header is V0
    public static final int CURRENT_HEADER_VERSION = V0;
    private final TreeMap<Long, SequenceGroup> sortedSequenceGroups = new TreeMap<Long, SequenceGroup>();
    private MutableObject<SequenceGroup> curSequenceGroup = new MutableObject<SequenceGroup>(null);
    private MutableLong curSequenceStartEntryId = new MutableLong(INVALID_ENTRYID);
    private MutableInt curSequenceSize = new MutableInt(0);
    private boolean availabilityOfEntriesOfLedgerClosed = false;
    private long totalNumOfAvailableEntries = 0;

    public AvailabilityOfEntriesOfLedger(PrimitiveIterator.OfLong entriesOfLedgerItr) {
        while (entriesOfLedgerItr.hasNext()) {
            this.addEntryToAvailabileEntriesOfLedger(entriesOfLedgerItr.nextLong());
        }
        this.closeStateOfEntriesOfALedger();
    }

    public AvailabilityOfEntriesOfLedger(long[] entriesOfLedger) {
        for (long entry : entriesOfLedger) {
            this.addEntryToAvailabileEntriesOfLedger(entry);
        }
        this.closeStateOfEntriesOfALedger();
    }

    public AvailabilityOfEntriesOfLedger(byte[] serializeStateOfEntriesOfLedger) {
        byte[] header = new byte[HEADER_SIZE];
        byte[] serializedSequenceGroupByteArray = new byte[SequenceGroup.SEQUENCEGROUP_BYTES];
        System.arraycopy(serializeStateOfEntriesOfLedger, 0, header, 0, HEADER_SIZE);

        ByteBuffer headerByteBuf = ByteBuffer.wrap(header);
        int headerVersion = headerByteBuf.getInt();
        if (headerVersion > CURRENT_HEADER_VERSION) {
            throw new IllegalArgumentException("Unsupported Header Version: " + headerVersion);
        }
        int numOfSequenceGroups = headerByteBuf.getInt();
        SequenceGroup newSequenceGroup;
        for (int i = 0; i < numOfSequenceGroups; i++) {
            Arrays.fill(serializedSequenceGroupByteArray, (byte) 0);
            System.arraycopy(serializeStateOfEntriesOfLedger, HEADER_SIZE + (i * SequenceGroup.SEQUENCEGROUP_BYTES),
                    serializedSequenceGroupByteArray, 0, SequenceGroup.SEQUENCEGROUP_BYTES);
            newSequenceGroup = new SequenceGroup(serializedSequenceGroupByteArray);
            sortedSequenceGroups.put(newSequenceGroup.getFirstSequenceStart(), newSequenceGroup);
        }
        setAvailabilityOfEntriesOfLedgerClosed();
    }

    public AvailabilityOfEntriesOfLedger(ByteBuf byteBuf) {
        byte[] header = new byte[HEADER_SIZE];
        byte[] serializedSequenceGroupByteArray = new byte[SequenceGroup.SEQUENCEGROUP_BYTES];
        int readerIndex = byteBuf.readerIndex();
        byteBuf.getBytes(readerIndex, header, 0, HEADER_SIZE);

        ByteBuffer headerByteBuf = ByteBuffer.wrap(header);
        int headerVersion = headerByteBuf.getInt();
        if (headerVersion > CURRENT_HEADER_VERSION) {
            throw new IllegalArgumentException("Unsupported Header Version: " + headerVersion);
        }
        int numOfSequenceGroups = headerByteBuf.getInt();
        SequenceGroup newSequenceGroup;
        for (int i = 0; i < numOfSequenceGroups; i++) {
            Arrays.fill(serializedSequenceGroupByteArray, (byte) 0);
            byteBuf.getBytes(readerIndex + HEADER_SIZE + (i * SequenceGroup.SEQUENCEGROUP_BYTES),
                    serializedSequenceGroupByteArray, 0, SequenceGroup.SEQUENCEGROUP_BYTES);
            newSequenceGroup = new SequenceGroup(serializedSequenceGroupByteArray);
            sortedSequenceGroups.put(newSequenceGroup.getFirstSequenceStart(), newSequenceGroup);
        }
        setAvailabilityOfEntriesOfLedgerClosed();
    }

    private void initializeCurSequence(long curSequenceStartEntryIdValue) {
        curSequenceStartEntryId.setValue(curSequenceStartEntryIdValue);
        curSequenceSize.setValue(1);
    }

    private void resetCurSequence() {
        curSequenceStartEntryId.setValue(INVALID_ENTRYID);
        curSequenceSize.setValue(0);
    }

    private boolean isCurSequenceInitialized() {
        return curSequenceStartEntryId.longValue() != INVALID_ENTRYID;
    }

    private boolean isEntryExistingInCurSequence(long entryId) {
        return (curSequenceStartEntryId.longValue() <= entryId)
                && (entryId < (curSequenceStartEntryId.longValue() + curSequenceSize.intValue()));
    }

    private boolean isEntryAppendableToCurSequence(long entryId) {
        return ((curSequenceStartEntryId.longValue() + curSequenceSize.intValue()) == entryId);
    }

    private void incrementCurSequenceSize() {
        curSequenceSize.increment();
    }

    private void createNewSequenceGroupWithCurSequence() {
        SequenceGroup curSequenceGroupValue = curSequenceGroup.getValue();
        curSequenceGroupValue.setSequenceGroupClosed();
        sortedSequenceGroups.put(curSequenceGroupValue.getFirstSequenceStart(), curSequenceGroupValue);
        curSequenceGroup.setValue(new SequenceGroup(curSequenceStartEntryId.longValue(), curSequenceSize.intValue()));
    }

    private boolean isCurSequenceGroupInitialized() {
        return curSequenceGroup.getValue() != null;
    }

    private void initializeCurSequenceGroupWithCurSequence() {
        curSequenceGroup.setValue(new SequenceGroup(curSequenceStartEntryId.longValue(), curSequenceSize.intValue()));
    }

    private boolean doesCurSequenceBelongToCurSequenceGroup() {
        long curSequenceStartEntryIdValue = curSequenceStartEntryId.longValue();
        int curSequenceSizeValue = curSequenceSize.intValue();
        boolean belongsToThisSequenceGroup = false;
        SequenceGroup curSequenceGroupValue = curSequenceGroup.getValue();
        if ((curSequenceGroupValue.sequenceSize == curSequenceSizeValue)
                && ((curSequenceGroupValue.getLastSequenceStart() == INVALID_ENTRYID) || ((curSequenceStartEntryIdValue
                        - curSequenceGroupValue.getLastSequenceStart()) == curSequenceGroupValue
                                .getSequencePeriod()))) {
            belongsToThisSequenceGroup = true;
        }
        return belongsToThisSequenceGroup;
    }

    private void appendCurSequenceToCurSequenceGroup() {
        SequenceGroup curSequenceGroupValue = curSequenceGroup.getValue();
        curSequenceGroupValue.setLastSequenceStart(curSequenceStartEntryId.longValue());
        if (curSequenceGroupValue.getSequencePeriod() == 0) {
            curSequenceGroupValue.setSequencePeriod(
                    ((int) (curSequenceGroupValue.getLastSequenceStart() - curSequenceGroupValue.firstSequenceStart)));
        }
    }

    private void addCurSequenceToSequenceGroup() {
        if (!isCurSequenceGroupInitialized()) {
            initializeCurSequenceGroupWithCurSequence();
        } else if (doesCurSequenceBelongToCurSequenceGroup()) {
            appendCurSequenceToCurSequenceGroup();
        } else {
            createNewSequenceGroupWithCurSequence();
        }
    }

    private void addEntryToAvailabileEntriesOfLedger(long entryId) {
        if (!isCurSequenceInitialized()) {
            initializeCurSequence(entryId);
        } else if (isEntryExistingInCurSequence(entryId)) {
            /* this entry is already added so do nothing */
        } else if (isEntryAppendableToCurSequence(entryId)) {
            incrementCurSequenceSize();
        } else {
            addCurSequenceToSequenceGroup();
            initializeCurSequence(entryId);
        }
    }

    private void closeStateOfEntriesOfALedger() {
        if (isCurSequenceInitialized()) {
            addCurSequenceToSequenceGroup();
            resetCurSequence();
        }
        SequenceGroup curSequenceGroupValue = curSequenceGroup.getValue();
        if (curSequenceGroupValue != null) {
            curSequenceGroupValue.setSequenceGroupClosed();
            sortedSequenceGroups.put(curSequenceGroupValue.getFirstSequenceStart(), curSequenceGroupValue);
        }
        setAvailabilityOfEntriesOfLedgerClosed();
    }

    private boolean isAvailabilityOfEntriesOfLedgerClosed() {
        return availabilityOfEntriesOfLedgerClosed;
    }

    private void setAvailabilityOfEntriesOfLedgerClosed() {
        this.availabilityOfEntriesOfLedgerClosed = true;
        for (Entry<Long, SequenceGroup> seqGroupEntry : sortedSequenceGroups.entrySet()) {
            totalNumOfAvailableEntries += seqGroupEntry.getValue().getNumOfEntriesInSequenceGroup();
        }
    }

    public byte[] serializeStateOfEntriesOfLedger() {
        if (!isAvailabilityOfEntriesOfLedgerClosed()) {
            throw new IllegalStateException("AvailabilityOfEntriesOfLedger is not yet closed,"
                    + "it is illegal to call serializeStateOfEntriesOfLedger");
        }
        byte[] header = new byte[HEADER_SIZE];
        ByteBuffer headerByteBuf = ByteBuffer.wrap(header);
        byte[] serializedSequenceGroupByteArray = new byte[SequenceGroup.SEQUENCEGROUP_BYTES];
        byte[] serializedStateByteArray = new byte[HEADER_SIZE
                + (sortedSequenceGroups.size() * SequenceGroup.SEQUENCEGROUP_BYTES)];
        final int numOfSequenceGroups = sortedSequenceGroups.size();
        headerByteBuf.putInt(CURRENT_HEADER_VERSION);
        headerByteBuf.putInt(numOfSequenceGroups);
        System.arraycopy(header, 0, serializedStateByteArray, 0, HEADER_SIZE);
        int seqNum = 0;
        for (Entry<Long, SequenceGroup> seqGroupEntry : sortedSequenceGroups.entrySet()) {
            SequenceGroup seqGroup = seqGroupEntry.getValue();
            Arrays.fill(serializedSequenceGroupByteArray, (byte) 0);
            seqGroup.serializeSequenceGroup(serializedSequenceGroupByteArray);
            System.arraycopy(serializedSequenceGroupByteArray, 0, serializedStateByteArray,
                    HEADER_SIZE + ((seqNum++) * SequenceGroup.SEQUENCEGROUP_BYTES), SequenceGroup.SEQUENCEGROUP_BYTES);
        }
        return serializedStateByteArray;
    }

    public boolean isEntryAvailable(long entryId) {
        if (!isAvailabilityOfEntriesOfLedgerClosed()) {
            throw new IllegalStateException(
                    "AvailabilityOfEntriesOfLedger is not yet closed, it is illegal to call isEntryAvailable");
        }
        Entry<Long, SequenceGroup> seqGroup = sortedSequenceGroups.floorEntry(entryId);
        if (seqGroup == null) {
            return false;
        }
        return seqGroup.getValue().isEntryAvailable(entryId);
    }

    public List<Long> getUnavailableEntries(long startEntryId, long lastEntryId, BitSet availabilityOfEntries) {
        if (!isAvailabilityOfEntriesOfLedgerClosed()) {
            throw new IllegalStateException(
                    "AvailabilityOfEntriesOfLedger is not yet closed, it is illegal to call getUnavailableEntries");
        }
        List<Long> unavailableEntries = new ArrayList<Long>();
        SequenceGroup curSeqGroup = null;
        boolean noSeqGroupRemaining = false;
        int bitSetIndex = 0;
        for (long entryId = startEntryId; entryId <= lastEntryId; entryId++, bitSetIndex++) {
            if (noSeqGroupRemaining) {
                if (availabilityOfEntries.get(bitSetIndex)) {
                    unavailableEntries.add(entryId);
                }
                continue;
            }
            if ((curSeqGroup == null) || (entryId > curSeqGroup.getLastEntryInSequenceGroup())) {
                Entry<Long, SequenceGroup> curSeqGroupEntry = sortedSequenceGroups.floorEntry(entryId);
                if (curSeqGroupEntry == null) {
                    if (availabilityOfEntries.get(bitSetIndex)) {
                        unavailableEntries.add(entryId);
                    }
                    if (sortedSequenceGroups.ceilingEntry(entryId) == null) {
                        noSeqGroupRemaining = true;
                    }
                    continue;
                } else {
                    curSeqGroup = curSeqGroupEntry.getValue();
                    if (entryId > curSeqGroup.getLastEntryInSequenceGroup()) {
                        if (availabilityOfEntries.get(bitSetIndex)) {
                            unavailableEntries.add(entryId);
                        }
                        noSeqGroupRemaining = true;
                        continue;
                    }
                }
            }
            if (availabilityOfEntries.get(bitSetIndex) && (!curSeqGroup.isEntryAvailable(entryId))) {
                unavailableEntries.add(entryId);
            }
        }
        return unavailableEntries;
    }

    public long getTotalNumOfAvailableEntries() {
        if (!isAvailabilityOfEntriesOfLedgerClosed()) {
            throw new IllegalStateException("AvailabilityOfEntriesOfLedger is not yet closed,"
                    + " it is illegal to call getTotalNumOfAvailableEntries");
        }
        return totalNumOfAvailableEntries;
    }
}
