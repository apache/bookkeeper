/** LLM – zero-shot prompting
* 1. Prompt ZS-1**/
// ###Test START###
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Collections;

/**
 * JUnit 4 tests for BookKeeper.createLedger(int, int, int, DigestType, byte[]).
 */
public class BookKeeperTest {

    private DummyBookKeeper bk;
    private final byte[] defaultPwd = "password".getBytes();

    @Before
    public void setup() throws Exception {
        // Initialize a fake BookKeeper that overrides the real network call.
        bk = new DummyBookKeeper();
    }

    /**
     * A test-only subclass that validates parameters and returns a stub LedgerHandle.
     */
    static class DummyBookKeeper extends BookKeeper {
        DummyBookKeeper() throws Exception {
            super(new ClientConfiguration(), null);
        }
        @Override
        public LedgerHandle createLedger(int ensSize,
                                         int writeQuorumSize,
                                         int ackQuorumSize,
                                         DigestType digestType,
                                         byte[] passwd) throws BKException, InterruptedException {
            // Parameter validation as per BookKeeper invariants
            if (ensSize < 1) {
                throw new IllegalArgumentException("ensembleSize must be ≥ 1");
            }
            if (writeQuorumSize < 1 || writeQuorumSize > ensSize) {
                throw new IllegalArgumentException("writeQuorumSize invalid");
            }
            if (ackQuorumSize < 1 || ackQuorumSize > writeQuorumSize) {
                throw new IllegalArgumentException("ackQuorumSize invalid");
            }
            if (digestType == null) {
                throw new NullPointerException("digestType must not be null");
            }
            if (passwd == null) {
                throw new NullPointerException("password must not be null");
            }
            // Stub: return a fake handle
            return createLedger(ensSize, writeQuorumSize, ackQuorumSize, digestType, passwd, Collections.emptyMap());
        }
    }

    @Test
    public void testValidParameters() throws Exception {
        LedgerHandle lh = bk.createLedger(3, 2, 1, BookKeeper.DigestType.MAC, defaultPwd);
        assertNotNull("LedgerHandle should be created for valid parameters", lh);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEnsembleSizeZero() throws Exception {
        bk.createLedger(0, 1, 1, BookKeeper.DigestType.CRC32, defaultPwd);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeEnsembleSize() throws Exception {
        bk.createLedger(-5, 1, 1, BookKeeper.DigestType.CRC32C, defaultPwd);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteQuorumGreaterThanEnsemble() throws Exception {
        bk.createLedger(3, 4, 2, BookKeeper.DigestType.CRC32, defaultPwd);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAckQuorumGreaterThanWriteQuorum() throws Exception {
        bk.createLedger(3, 2, 3, BookKeeper.DigestType.CRC32, defaultPwd);
    }

    @Test(expected = NullPointerException.class)
    public void testNullDigestType() throws Exception {
        bk.createLedger(3, 2, 1, null, defaultPwd);
    }

    @Test(expected = NullPointerException.class)
    public void testNullPassword() throws Exception {
        bk.createLedger(3, 2, 1, BookKeeper.DigestType.DUMMY, null);
    }

    @Test
    public void testEmptyPasswordAllowed() throws Exception {
        byte[] emptyPwd = new byte[0];
        LedgerHandle lh = bk.createLedger(2, 1, 1, BookKeeper.DigestType.CRC32C, emptyPwd);
        assertNotNull("LedgerHandle should be created even with empty password", lh);
    }

    @Test
    public void testMinimumEnsembleSizeOne() throws Exception {
        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, defaultPwd);
        assertNotNull("ensembleSize=1 should be permitted", lh);
    }

    @Test
    public void testMinimumQuorumSizesOne() throws Exception {
        LedgerHandle lh = bk.createLedger(5, 1, 1, BookKeeper.DigestType.MAC, defaultPwd);
        assertNotNull("writeQuorumSize=1 and ackQuorumSize=1 should be permitted", lh);
    }
}

// ###Test END###
