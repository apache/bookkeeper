package org.apache.bookkeeper.meta;

import java.io.IOException;

import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.meta.LedgerManager.LedgerRangeIterator;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.ZooKeeper;

public class HierarchicalLedgerManager extends AbstractHierarchicalLedgerManager {
    static final Logger LOG = LoggerFactory.getLogger(HierarchicalLedgerManager.class);

    LegacyHierarchicalLedgerManager legacyLM;
    LongHierarchicalLedgerManager longLM;

    public HierarchicalLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);
        legacyLM = new LegacyHierarchicalLedgerManager(conf, zk);
        longLM = new LongHierarchicalLedgerManager (conf, zk);
    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context, int successRc,
            int failureRc) {
        // Process the old 31-bit id ledgers first.
        legacyLM.asyncProcessLedgers(processor, new VoidCallback(){

            @Override
            public void processResult(int rc, String path, Object ctx) {
                if(rc == failureRc) {
                    // If it fails, return the failure code to the callback
                    finalCb.processResult(rc, path, ctx);
                }
                else {
                    // If it succeeds, proceed with our own recursive ledger processing for the 63-bit id ledgers
                    longLM.asyncProcessLedgers(processor, finalCb, context, successRc, failureRc);
                }
            }

        }, context, successRc, failureRc);
    }

    @Override
    protected String getLedgerPath(long ledgerId) {
        return ledgerRootPath + StringUtils.getHybridHierarchicalLedgerPath(ledgerId);
    }

    @Override
    protected long getLedgerId(String ledgerPath) throws IOException {
        // TODO Auto-generated method stub
        if (!ledgerPath.startsWith(ledgerRootPath)) {
            throw new IOException("it is not a valid hashed path name : " + ledgerPath);
        }
        String hierarchicalPath = ledgerPath.substring(ledgerRootPath.length() + 1);
        return StringUtils.stringToLongHierarchicalLedgerId(hierarchicalPath);
    }

    @Override
    public LedgerRangeIterator getLedgerRanges() {
        LedgerRangeIterator legacyLedgerRangeIterator = legacyLM.getLedgerRanges();
        LedgerRangeIterator longLedgerRangeIterator = longLM.getLedgerRanges();
        return new HierarchicalLedgerRangeIterator(legacyLedgerRangeIterator, longLedgerRangeIterator);
    }

    private class HierarchicalLedgerRangeIterator implements LedgerRangeIterator {

        LedgerRangeIterator legacyLedgerRangeIterator;
        LedgerRangeIterator longLedgerRangeIterator;

        HierarchicalLedgerRangeIterator(LedgerRangeIterator legacyLedgerRangeIterator, LedgerRangeIterator longLedgerRangeIterator) {
            this.legacyLedgerRangeIterator = legacyLedgerRangeIterator;
            this.longLedgerRangeIterator = longLedgerRangeIterator;
        }

        @Override
        public boolean hasNext() throws IOException {
            return legacyLedgerRangeIterator.hasNext() || longLedgerRangeIterator.hasNext();
        }

        @Override
        public LedgerRange next() throws IOException {
            if(legacyLedgerRangeIterator.hasNext()) {
                return legacyLedgerRangeIterator.next();
            }
            return longLedgerRangeIterator.next();
        }

    }
}
