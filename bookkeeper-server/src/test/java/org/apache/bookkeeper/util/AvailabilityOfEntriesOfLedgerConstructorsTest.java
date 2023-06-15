package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class AvailabilityOfEntriesOfLedgerConstructorsTest {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger; // tested object
    private final long[] bookieContent;

    @Parameterized.Parameters
    public static Collection<Object[]> data(){

        long[] bookieContent = new long[]{1L, 3L, 4L};
        long[] bookieContentWithSequencePeriod = new long[]{1L, 3L, 5L, 7L};
        long[] bookieContent3 = {3, 4, 5, 5, 10, 11, 12, 13, 6, 7, 8};


        return Arrays.asList(new Object[][]{
                {bookieContent},
                {new long[]{}},
                {null},

                //JACOCO
                {bookieContentWithSequencePeriod},
                {bookieContent3}
        });
    }

    public AvailabilityOfEntriesOfLedgerConstructorsTest(long[] bookieContent){
        this.bookieContent = bookieContent;
    }

    @Test
    public void testConstructor1() {
        try {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.bookieContent);

            Assert.assertEquals(Arrays.stream(bookieContent).distinct().count(), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }
        } catch (Exception e) {
            if(this.bookieContent == null) return;
            Assert.fail();
        }
    }

    @Test
    public void testConstructor2() {
        try {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.bookieContent);

            Assert.assertEquals(Arrays.stream(bookieContent).distinct().count(), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }

            byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();

            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(serializedState);

            Assert.assertEquals(Arrays.stream(bookieContent).distinct().count(), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }
        }  catch (Exception e) {
            if(this.bookieContent == null) return;
            Assert.fail();
        }
    }

    @Test
    public void testConstructor3() {
        try {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.bookieContent);

            ByteBuf byteBuf = Unpooled.buffer();
            byteBuf.writeBytes(availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger());
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);

            Assert.assertEquals(Arrays.stream(bookieContent).distinct().count(), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }
        }  catch (Exception e) {
            if(this.bookieContent == null) return;
            Assert.fail();
        }
    }
}
