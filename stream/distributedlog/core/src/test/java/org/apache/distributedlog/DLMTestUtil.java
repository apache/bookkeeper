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
package org.apache.distributedlog;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.feature.SettableFeatureProvider;
import org.apache.bookkeeper.versioning.Version;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.common.util.PermitLimiter;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.impl.logsegment.BKLogSegmentEntryWriter;
import org.apache.distributedlog.logsegment.LogSegmentEntryStore;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.distributedlog.util.ConfUtils;
import org.apache.distributedlog.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for setting up bookkeeper ensembles
 * and bringing individual bookies up and down.
 */
public class DLMTestUtil {
    protected static final Logger LOG = LoggerFactory.getLogger(DLMTestUtil.class);
    private static final  byte[] payloadStatic = repeatString("abc", 512).getBytes();

    static String repeatString(String s, int n) {
        StringBuilder ret = new StringBuilder(s);
        for (int i = 1; i < n; i++) {
            ret.append(s);
        }
        return ret.toString();
    }

    public static Map<Long, LogSegmentMetadata> readLogSegments(ZooKeeperClient zkc, String ledgerPath)
            throws Exception {
        List<String> children = zkc.get().getChildren(ledgerPath, false);
        LOG.info("Children under {} : {}", ledgerPath, children);
        Map<Long, LogSegmentMetadata> segments =
            new HashMap<Long, LogSegmentMetadata>(children.size());
        for (String child : children) {
            LogSegmentMetadata segment =
                    Utils.ioResult(LogSegmentMetadata.read(zkc, ledgerPath + "/" + child));
            LOG.info("Read segment {} : {}", child, segment);
            segments.put(segment.getLogSegmentSequenceNumber(), segment);
        }
        return segments;
    }

    public static URI createDLMURI(int port, String path) throws Exception {
        return LocalDLMEmulator.createDLMURI("127.0.0.1:" + port, path);
    }

    public static DistributedLogManager createNewDLM(String name,
                                                     DistributedLogConfiguration conf,
                                                     URI uri) throws Exception {
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();
        return namespace.openLog(name);
    }

    @SuppressWarnings("deprecation")
    static org.apache.distributedlog.api.MetadataAccessor createNewMetadataAccessor(
            DistributedLogConfiguration conf,
            String name,
            URI uri) throws Exception {
        // TODO: Metadata Accessor seems to be a legacy object which only used by kestrel
        //       (we might consider deprecating this)
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();
        return namespace.getNamespaceDriver().getMetadataAccessor(name);
    }

    public static void fenceStream(DistributedLogConfiguration conf, URI uri, String name) throws Exception {
        DistributedLogManager dlm = createNewDLM(name, conf, uri);
        try {
            List<LogSegmentMetadata> logSegmentList = dlm.getLogSegments();
            LogSegmentMetadata lastSegment = logSegmentList.get(logSegmentList.size() - 1);
            LogSegmentEntryStore entryStore =
                    dlm.getNamespaceDriver().getLogSegmentEntryStore(NamespaceDriver.Role.READER);
            Utils.close(Utils.ioResult(entryStore.openRandomAccessReader(lastSegment, true)));
        } finally {
            dlm.close();
        }
    }

    static long getNumberofLogRecords(DistributedLogManager bkdlm, long startTxId) throws IOException {
        long numLogRecs = 0;
        LogReader reader = bkdlm.getInputStream(startTxId);
        LogRecord record = reader.readNext(false);
        while (null != record) {
            numLogRecs++;
            verifyLogRecord(record);
            record = reader.readNext(false);
        }
        reader.close();
        return numLogRecs;
    }

    public static LogRecord getLogRecordInstance(long txId) {
        return new LogRecord(txId, generatePayload(txId));
    }

    public static LogRecord getLogRecordInstance(long txId, int size) {
        ByteBuffer buf = ByteBuffer.allocate(size);
        return new LogRecord(txId, buf.array());
    }

    public static void verifyLogRecord(LogRecord record) {
        assertEquals(generatePayload(record.getTransactionId()).length, record.getPayload().length);
        assertArrayEquals(generatePayload(record.getTransactionId()), record.getPayload());
        assertTrue(!record.isControl());
        verifyPayload(record.getTransactionId(), record.getPayload());
    }

    static byte[] generatePayload(long txId) {
        return String.format("%d;%d", txId, txId).getBytes();
    }

    static void verifyPayload(long txId, byte[] payload) {
        String[] txIds = new String(payload).split(";");
        assertEquals(Long.valueOf(txIds[0]), Long.valueOf(txIds[0]));
    }

    static LogRecord getLargeLogRecordInstance(long txId, boolean control) {
        LogRecord record = new LogRecord(txId, payloadStatic);
        if (control) {
            record.setControl();
        }
        return record;
    }

    static LogRecord getLargeLogRecordInstance(long txId) {
        return new LogRecord(txId, payloadStatic);
    }

    static List<LogRecord> getLargeLogRecordInstanceList(long firstTxId, int count) {
        List<LogRecord> logrecs = new ArrayList<LogRecord>(count);
        for (long i = 0; i < count; i++) {
            logrecs.add(getLargeLogRecordInstance(firstTxId + i));
        }
        return logrecs;
    }

    static List<LogRecord> getLogRecordInstanceList(long firstTxId, int count, int size) {
        List<LogRecord> logrecs = new ArrayList<LogRecord>(count);
        for (long i = 0; i < count; i++) {
            logrecs.add(getLogRecordInstance(firstTxId + i, size));
        }
        return logrecs;
    }

    static void verifyLargeLogRecord(LogRecord record) {
        verifyLargeLogRecord(record.getPayload());
    }

    static void verifyLargeLogRecord(byte[] payload) {
        assertArrayEquals(payloadStatic, payload);
    }

    static LogRecord getEmptyLogRecordInstance(long txId) {
        return new LogRecord(txId, new byte[0]);
    }

    static void verifyEmptyLogRecord(LogRecord record) {
        assertEquals(record.getPayload().length, 0);
    }

    public static LogRecordWithDLSN getLogRecordWithDLSNInstance(DLSN dlsn, long txId) {
        return getLogRecordWithDLSNInstance(dlsn, txId, false);
    }

    public static LogRecordWithDLSN getLogRecordWithDLSNInstance(DLSN dlsn, long txId, boolean isControlRecord) {
        LogRecordWithDLSN record = new LogRecordWithDLSN(dlsn, txId, generatePayload(txId), 1L);
        record.setPositionWithinLogSegment((int) txId + 1);
        if (isControlRecord) {
            record.setControl();
        }
        return record;
    }

    public static String inprogressZNodeName(long logSegmentSeqNo) {
        return String.format("%s_%018d", DistributedLogConstants.INPROGRESS_LOGSEGMENT_PREFIX, logSegmentSeqNo);
    }

    public static String completedLedgerZNodeNameWithVersion(long ledgerId,
                                                             long firstTxId, long lastTxId, long logSegmentSeqNo) {
        return String.format("%s_%018d_%018d_%018d_v%dl%d_%04d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX,
                       firstTxId, lastTxId, logSegmentSeqNo, DistributedLogConstants.LOGSEGMENT_NAME_VERSION, ledgerId,
                       DistributedLogConstants.LOCAL_REGION_ID);
    }

    public static String completedLedgerZNodeNameWithTxID(long firstTxId, long lastTxId) {
        return String.format("%s_%018d_%018d",
                DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, firstTxId, lastTxId);
    }

    public static String completedLedgerZNodeNameWithLogSegmentSequenceNumber(long logSegmentSeqNo) {
        return String.format("%s_%018d", DistributedLogConstants.COMPLETED_LOGSEGMENT_PREFIX, logSegmentSeqNo);
    }

    public static LogSegmentMetadata inprogressLogSegment(String ledgerPath,
                                                          long ledgerId,
                                                          long firstTxId,
                                                          long logSegmentSeqNo) {
        return inprogressLogSegment(ledgerPath, ledgerId, firstTxId, logSegmentSeqNo,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    public static LogSegmentMetadata inprogressLogSegment(String ledgerPath,
                                                          long ledgerId,
                                                          long firstTxId,
                                                          long logSegmentSeqNo,
                                                          int version) {
        return new LogSegmentMetadata.LogSegmentMetadataBuilder(
                    ledgerPath + "/" + inprogressZNodeName(logSegmentSeqNo),
                    version,
                    ledgerId,
                    firstTxId)
                .setLogSegmentSequenceNo(logSegmentSeqNo)
                .build();
    }

    public static LogSegmentMetadata completedLogSegment(String ledgerPath,
                                                         long ledgerId,
                                                         long firstTxId,
                                                         long lastTxId,
                                                         int recordCount,
                                                         long logSegmentSeqNo,
                                                         long lastEntryId,
                                                         long lastSlotId) {
        return completedLogSegment(ledgerPath, ledgerId, firstTxId, lastTxId,
                recordCount, logSegmentSeqNo, lastEntryId, lastSlotId,
                LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    }

    public static LogSegmentMetadata completedLogSegment(String ledgerPath,
                                                         long ledgerId,
                                                         long firstTxId,
                                                         long lastTxId,
                                                         int recordCount,
                                                         long logSegmentSeqNo,
                                                         long lastEntryId,
                                                         long lastSlotId,
                                                         int version) {
        LogSegmentMetadata metadata =
                new LogSegmentMetadata.LogSegmentMetadataBuilder(
                        ledgerPath + "/" + inprogressZNodeName(logSegmentSeqNo),
                        version,
                        ledgerId,
                        firstTxId)
                    .setInprogress(false)
                    .setLogSegmentSequenceNo(logSegmentSeqNo)
                    .build();
        return metadata.completeLogSegment(ledgerPath + "/"
                        + completedLedgerZNodeNameWithLogSegmentSequenceNumber(logSegmentSeqNo),
                lastTxId, recordCount, lastEntryId, lastSlotId, firstTxId);
    }

    public static void generateCompletedLogSegments(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                    long numCompletedSegments, long segmentSize) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        long txid = 1L;
        for (long i = 0; i < numCompletedSegments; i++) {
            BKSyncLogWriter writer = dlm.startLogSegmentNonPartitioned();
            for (long j = 1; j <= segmentSize; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            writer.closeAndComplete();
        }
    }

    public static long generateLogSegmentNonPartitioned(DistributedLogManager dlm,
                                                 int controlEntries, int userEntries, long startTxid) throws Exception {
        return generateLogSegmentNonPartitioned(dlm, controlEntries, userEntries, startTxid, 1L);
    }

    public static long generateLogSegmentNonPartitioned(DistributedLogManager dlm, int controlEntries,
                                                   int userEntries, long startTxid, long txidStep) throws Exception {
        AsyncLogWriter out = dlm.startAsyncLogSegmentNonPartitioned();
        long txid = startTxid;
        for (int i = 0; i < controlEntries; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            record.setControl();
            Utils.ioResult(out.write(record));
            txid += txidStep;
        }
        for (int i = 0; i < userEntries; ++i) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(txid);
            Utils.ioResult(out.write(record));
            txid += txidStep;
        }
        Utils.close(out);
        return txid - startTxid;
    }

    public static ZooKeeperClient getZooKeeperClient(BKDistributedLogManager dlm) {
        return ((BKNamespaceDriver) dlm.getNamespaceDriver()).getWriterZKC();
    }

    public static BookKeeperClient getBookKeeperClient(BKDistributedLogManager dlm) {
        return ((BKNamespaceDriver) dlm.getNamespaceDriver()).getReaderBKC();
    }

    public static void injectLogSegmentWithGivenLogSegmentSeqNo(DistributedLogManager manager,
                                                DistributedLogConfiguration conf, long logSegmentSeqNo, long startTxID,
                                                boolean writeEntries, long segmentSize, boolean completeLogSegment)
            throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        BKLogWriteHandler writeHandler = dlm.createWriteHandler(false);
        Utils.ioResult(writeHandler.lockHandler());
        // Start a log segment with a given ledger seq number.
        BookKeeperClient bkc = getBookKeeperClient(dlm);
        LedgerHandle lh = bkc.get().createLedger(conf.getEnsembleSize(), conf.getWriteQuorumSize(),
                conf.getAckQuorumSize(), BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes());
        String inprogressZnodeName = writeHandler.inprogressZNodeName(lh.getId(), startTxID, logSegmentSeqNo);
        String znodePath = writeHandler.inprogressZNode(lh.getId(), startTxID, logSegmentSeqNo);
        int logSegmentMetadataVersion = conf.getDLLedgerMetadataLayoutVersion();
        LogSegmentMetadata l =
            new LogSegmentMetadata.LogSegmentMetadataBuilder(znodePath,
                    logSegmentMetadataVersion, lh.getId(), startTxID)
                .setLogSegmentSequenceNo(logSegmentSeqNo)
                .setEnvelopeEntries(LogSegmentMetadata.supportsEnvelopedEntries(logSegmentMetadataVersion))
                .build();
        l.write(getZooKeeperClient(dlm));
        writeHandler.maxTxId.update(Version.ANY, startTxID);
        writeHandler.addLogSegmentToCache(inprogressZnodeName, l);
        BKLogSegmentWriter writer = new BKLogSegmentWriter(
                writeHandler.getFullyQualifiedName(),
                inprogressZnodeName,
                conf,
                conf.getDLLedgerMetadataLayoutVersion(),
                new BKLogSegmentEntryWriter(lh),
                writeHandler.lock,
                startTxID,
                logSegmentSeqNo,
                writeHandler.scheduler,
                writeHandler.statsLogger,
                writeHandler.statsLogger,
                writeHandler.alertStatsLogger,
                PermitLimiter.NULL_PERMIT_LIMITER,
                new SettableFeatureProvider("", 0),
                ConfUtils.getConstDynConf(conf));
        if (writeEntries) {
            long txid = startTxID;
            for (long j = 1; j <= segmentSize; j++) {
                writer.write(DLMTestUtil.getLogRecordInstance(txid++));
            }
            Utils.ioResult(writer.flushAndCommit());
        }
        if (completeLogSegment) {
            Utils.ioResult(writeHandler.completeAndCloseLogSegment(writer));
        }
        Utils.ioResult(writeHandler.unlockHandler());
    }

    public static void injectLogSegmentWithLastDLSN(DistributedLogManager manager, DistributedLogConfiguration conf,
                                                    long logSegmentSeqNo, long startTxID, long segmentSize,
                                                    boolean recordWrongLastDLSN) throws Exception {
        BKDistributedLogManager dlm = (BKDistributedLogManager) manager;
        BKLogWriteHandler writeHandler = dlm.createWriteHandler(false);
        Utils.ioResult(writeHandler.lockHandler());
        // Start a log segment with a given ledger seq number.
        BookKeeperClient bkc = getBookKeeperClient(dlm);
        LedgerHandle lh = bkc.get().createLedger(conf.getEnsembleSize(), conf.getWriteQuorumSize(),
                conf.getAckQuorumSize(), BookKeeper.DigestType.CRC32, conf.getBKDigestPW().getBytes());
        String inprogressZnodeName = writeHandler.inprogressZNodeName(lh.getId(), startTxID, logSegmentSeqNo);
        String znodePath = writeHandler.inprogressZNode(lh.getId(), startTxID, logSegmentSeqNo);
        LogSegmentMetadata l =
            new LogSegmentMetadata.LogSegmentMetadataBuilder(znodePath,
                conf.getDLLedgerMetadataLayoutVersion(), lh.getId(), startTxID)
            .setLogSegmentSequenceNo(logSegmentSeqNo)
            .setInprogress(false)
            .build();
        l.write(getZooKeeperClient(dlm));
        writeHandler.maxTxId.update(Version.ANY, startTxID);
        writeHandler.addLogSegmentToCache(inprogressZnodeName, l);
        BKLogSegmentWriter writer = new BKLogSegmentWriter(
                writeHandler.getFullyQualifiedName(),
                inprogressZnodeName,
                conf,
                conf.getDLLedgerMetadataLayoutVersion(),
                new BKLogSegmentEntryWriter(lh),
                writeHandler.lock,
                startTxID,
                logSegmentSeqNo,
                writeHandler.scheduler,
                writeHandler.statsLogger,
                writeHandler.statsLogger,
                writeHandler.alertStatsLogger,
                PermitLimiter.NULL_PERMIT_LIMITER,
                new SettableFeatureProvider("", 0),
                ConfUtils.getConstDynConf(conf));
        long txid = startTxID;
        DLSN wrongDLSN = null;
        for (long j = 1; j <= segmentSize; j++) {
            DLSN dlsn = Utils.ioResult(writer.asyncWrite(DLMTestUtil.getLogRecordInstance(txid++)));
            if (j == (segmentSize - 1)) {
                wrongDLSN = dlsn;
            }
        }
        assertNotNull(wrongDLSN);
        if (recordWrongLastDLSN) {
            Utils.ioResult(writer.asyncClose());
            writeHandler.completeAndCloseLogSegment(
                    writeHandler.inprogressZNodeName(writer.getLogSegmentId(),
                            writer.getStartTxId(), writer.getLogSegmentSequenceNumber()),
                    writer.getLogSegmentSequenceNumber(),
                    writer.getLogSegmentId(),
                    writer.getStartTxId(),
                    startTxID + segmentSize - 2,
                    writer.getPositionWithinLogSegment() - 1,
                    wrongDLSN.getEntryId(),
                    wrongDLSN.getSlotId());
        } else {
            Utils.ioResult(writeHandler.completeAndCloseLogSegment(writer));
        }
        Utils.ioResult(writeHandler.unlockHandler());
    }

    public static void updateSegmentMetadata(ZooKeeperClient zkc, LogSegmentMetadata segment) throws Exception {
        byte[] finalisedData = segment.getFinalisedData().getBytes(UTF_8);
        zkc.get().setData(segment.getZkPath(), finalisedData, -1);
    }

    public static ServerConfiguration loadTestBkConf() {
        ServerConfiguration conf = new ServerConfiguration();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        URL confUrl = classLoader.getResource("bk_server.conf");
        try {
            if (null != confUrl) {
                conf.loadConf(confUrl);
                LOG.info("loaded bk_server.conf from resources");
            }
        } catch (org.apache.commons.configuration.ConfigurationException ex) {
            LOG.warn("loading conf failed", ex);
        }
        conf.setAllowLoopback(true);
        return conf;
    }

    public static <T> void validateFutureFailed(CompletableFuture<T> future, Class exClass) {
        try {
            Utils.ioResult(future);
        } catch (Exception ex) {
            LOG.info("Expected: {} Actual: {}", exClass.getName(), ex.getClass().getName());
            assertTrue("exceptions types equal", exClass.isInstance(ex));
        }
    }

    public static <T> T validateFutureSucceededAndGetResult(CompletableFuture<T> future) throws Exception {
        try {
            return Utils.ioResult(future, 10, TimeUnit.SECONDS);
        } catch (Exception ex) {
            fail("unexpected exception " + ex.getClass().getName());
            throw ex;
        }
    }
}
