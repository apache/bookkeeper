package org.apache.bookkeeper.client;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class CreateLedgerPrimitiveParametrizedTest {

    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private BookKeeper.DigestType digestType;
    private byte[] passwd;
    private AsyncCallback.CreateCallback cb;
    private Object ctx;
    private Map<String, byte[]> customMetadata;

    private boolean isTestPassed;

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{
                {1, 1, 1, BookKeeper.DigestType.CRC32},

                {-1, -1, -1, BookKeeper.DigestType.MAC},
//                {-1, -1, 0, BookKeeper.DigestType.DUMMY},
//                {-1, 0, 0, BookKeeper.DigestType.DUMMY},
//                {-1, 0, 1, BookKeeper.DigestType.DUMMY},
//                {0, 0, 0, BookKeeper.DigestType.CRC32C},
//                {0, 0, 1, BookKeeper.DigestType.DUMMY},
//                {0, 1, 1, BookKeeper.DigestType.DUMMY},
//                {0, 1, 2, BookKeeper.DigestType.DUMMY},
//                {1, 1, 1, BookKeeper.DigestType.CRC32},
//                {1, 1, 2, BookKeeper.DigestType.DUMMY},
//                {1, 2, 2, BookKeeper.DigestType.DUMMY},
//                {1, 2, 3, BookKeeper.DigestType.DUMMY},
//                {1, 0, -1, BookKeeper.DigestType.DUMMY}
        });

    }

    public CreateLedgerPrimitiveParametrizedTest(int e, int w, int a, BookKeeper.DigestType digestType) {
        this.ensSize = e;
        this.writeQuorumSize = w;
        this.ackQuorumSize = a;
        this.digestType = digestType;

        this.isTestPassed = ensSize >= writeQuorumSize && writeQuorumSize >= ackQuorumSize;

    }


    @Before
    public void setUp() {
        this.passwd = "testPasswd".getBytes();
        this.cb = null;
        this.ctx = null;
        this.customMetadata = Collections.emptyMap();


        //debug
//        this.ensSize = -1;
//        this.ackQuorumSize = -1;
//        this.writeQuorumSize = -1;
//        this.digestType = BookKeeper.DigestType.DUMMY;
//        this.isTestPassed = ensSize >= writeQuorumSize && writeQuorumSize >= ackQuorumSize;

    }


    //@Test
    public void testCreateLedgerAsync() throws BKException, IOException, InterruptedException {
        BookKeeper bookKeeper = new BookKeeper("localhost:2181");

        boolean wasExceptionCaught = false;
        try {
            bookKeeper.asyncCreateLedger(
                    this.ensSize,
                    this.writeQuorumSize,
                    this.ackQuorumSize,
                    this.digestType,
                    this.passwd,
                    (AsyncCallback.CreateCallback) (rc, lh, ctx) -> {
                        if (rc != 0) {
                            fail("Failed to create ledger. Error code: " + rc);
                        } else {
                            assertNotNull(lh);
                            assertTrue(lh.getId() > 0);
                        }
                        CountDownLatch latch = new CountDownLatch(1);
                        latch.countDown();
                    },
                    new Object(),
                    customMetadata
                    );
        } catch (Exception e) {
            wasExceptionCaught = true;
            e.printStackTrace();

        }

        assertEquals(this.isTestPassed, !wasExceptionCaught);

    }

    @Test
    public void testOfATest() {

        BookKeeper bookKeeper = mock(BookKeeper.class);
        Mockito.when(bookKeeper.getReturnRc(anyInt())).thenReturn(1);
        assertEquals(1, bookKeeper.getReturnRc(90));

    }
}