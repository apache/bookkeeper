package org.apache.hedwig.server.netty;

import static org.junit.Assert.assertEquals;

import org.apache.hedwig.server.netty.ServerStats.OpStats;
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
        OpStats opStat = new OpStats();
        opStat.updateLatency(-10);
        assertEquals("Should not update any latency metrics", 0,
                opStat.numSuccessOps);

    }
}
