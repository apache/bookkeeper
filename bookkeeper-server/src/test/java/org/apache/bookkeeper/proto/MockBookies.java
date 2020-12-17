package org.apache.bookkeeper.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.DistributionSchedule;
import org.apache.bookkeeper.client.RoundRobinDistributionSchedule;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;

public class MockBookies {
    static final Logger LOG = LoggerFactory.getLogger(MockBookies.class);
    final ConcurrentHashMap<BookieId, ConcurrentHashMap<Long, MockLedgerData>> data = new ConcurrentHashMap<>();

    public void seedLedgerForBookie(BookieId bookieId, long ledgerId,
                                    LedgerMetadata metadata) throws Exception {
        seedLedgerBase(ledgerId, metadata, (_bookie, entry) -> _bookie.equals(bookieId));
    }

    public void seedLedger(long ledgerId, LedgerMetadata metadata) throws Exception {
        seedLedgerBase(ledgerId, metadata, (_bookie, entry) -> true);
    }

    public void seedLedgerBase(long ledgerId, LedgerMetadata metadata,
                               BiPredicate<BookieId, Long> shouldSeed) throws Exception {
        DistributionSchedule schedule = new RoundRobinDistributionSchedule(metadata.getWriteQuorumSize(),
                metadata.getAckQuorumSize(),
                metadata.getEnsembleSize());
        long lastEntry = metadata.isClosed()
                ? metadata.getLastEntryId() : metadata.getAllEnsembles().lastEntry().getKey() - 1;
        long lac = -1;
        for (long e = 0; e <= lastEntry; e++) {
            List<BookieId> ensemble = metadata.getEnsembleAt(e);
            DistributionSchedule.WriteSet ws = schedule.getWriteSet(e);
            for (int i = 0; i < ws.size(); i++) {
                BookieId bookieId = ensemble.get(ws.get(i));
                if (shouldSeed.test(bookieId, e)) {
                    seedEntries(bookieId, ledgerId, e, lac);
                }
            }
            lac = e;
        }
    }

    public void seedEntries(BookieId bookieId, long ledgerId, long entryId, long lac) throws Exception {
        ByteBuf entry = generateEntry(ledgerId, entryId, lac);
        MockLedgerData ledger = getBookieData(bookieId).computeIfAbsent(ledgerId, MockLedgerData::new);
        ledger.addEntry(entryId, entry);
    }

    public ByteBuf generateEntry(long ledgerId, long entryId, long lac) throws Exception {
        DigestManager digestManager = DigestManager.instantiate(ledgerId, new byte[0], DataFormats.LedgerMetadataFormat.DigestType.CRC32C,
                UnpooledByteBufAllocator.DEFAULT, false);
        return ByteBufList.coalesce(digestManager.computeDigestAndPackageForSending(
                entryId, lac, 0, Unpooled.buffer(10)));

    }

    public void addEntry(BookieId bookieId, long ledgerId, long entryId, ByteBuf entry) throws BKException {
        MockLedgerData ledger = getBookieData(bookieId).computeIfAbsent(ledgerId, MockLedgerData::new);
        if(ledger.isFenced()) {
            throw new BKException.BKLedgerFencedException();
        }
        ledger.addEntry(entryId, entry);
    }

    public void recoveryAddEntry(BookieId bookieId, long ledgerId, long entryId, ByteBuf entry) throws BKException {
        MockLedgerData ledger = getBookieData(bookieId).computeIfAbsent(ledgerId, MockLedgerData::new);
        ledger.addEntry(entryId, entry);
    }

    public ByteBuf readEntry(BookieId bookieId, int flags, long ledgerId, long entryId) throws BKException {
        MockLedgerData ledger = getBookieData(bookieId).get(ledgerId);

        if (ledger == null) {
            LOG.warn("[{};L{}] ledger not found", bookieId, ledgerId);
            throw new BKException.BKNoSuchLedgerExistsException();
        }

        if((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING) {
            ledger.fence();
        }

        ByteBuf entry = ledger.getEntry(entryId);
        if (entry == null) {
            LOG.warn("[{};L{}] entry({}) not found", bookieId, ledgerId, entryId);
            throw new BKException.BKNoSuchEntryException();
        }

        return entry;
    }

    public ConcurrentHashMap<Long, MockLedgerData> getBookieData(BookieId bookieId) {
        return data.computeIfAbsent(bookieId, (key) -> new ConcurrentHashMap<>());
    }


}
