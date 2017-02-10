package org.apache.bookkeeper.meta;

public class HierarchicalLedgerManagerFactory extends LegacyHierarchicalLedgerManagerFactory {

    public static final String NAME = "hierarchical";

    @Override
    public LedgerManager newLedgerManager() {
        return new HierarchicalLedgerManager(conf, zk);
    }
    
    @Override
    public LedgerIdGenerator newLedgerIdGenerator() {
        ZkLedgerIdGenerator subIdGenerator = new ZkLedgerIdGenerator(zk, conf.getZkLedgersRootPath(), LegacyHierarchicalLedgerManager.IDGEN_ZNODE);
        return new LongZkLedgerIdGenerator(zk, conf.getZkLedgersRootPath(), LongHierarchicalLedgerManager.IDGEN_ZNODE, subIdGenerator);
    }
    
}
