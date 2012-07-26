package org.apache.hedwig.server.netty;

import static org.junit.Assert.assertEquals;

import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.junit.Test;

/** Tests that Statistics updation in hedwig Server */
public class TestServerStats {

    /**
     * Tests that updatLatency should not fail with
     * ArrayIndexOutOfBoundException when latency time coming as negative.
     */
    @Test
    public void testUpdateLatencyShouldNotFailWithAIOBEWithNegativeLatency()
            throws Exception {
        ServerStats stats = ServerStats.getInstance();
        org.apache.hedwig.server.netty.ServerStats.OpStats opStat = stats
                .getOpStats(OperationType.SUBSCRIBE);
        opStat.updateLatency(-10);
        assertEquals("Should not update any latency metrics", 0,
                opStat.numSuccessOps);

    }
}
