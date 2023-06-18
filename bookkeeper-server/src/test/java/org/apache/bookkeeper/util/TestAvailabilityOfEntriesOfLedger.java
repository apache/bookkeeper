package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedgerConstructorsTest.countElem;

public class TestAvailabilityOfEntriesOfLedger {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger; // tested object

    //Ba-dua: the case when the header is invalid during the deserialization did not cover, so this test cover that situation.
    @Test
    public void testConstructor1() {
        try {

            ByteBuf byteBuf = Unpooled.buffer();
            for(int i = 0; i < 64; i++)
                byteBuf.writeByte(1);
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);

        }  catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }
        Assert.fail();
    }

    //Ba-dua
    @Test
    public void testConstructor2() {
        try {

            byte[] byteBuf = new byte[64];
            for(int i = 0; i < 64; i++)
                byteBuf[i] = 1;
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);

        }  catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            return;
        }
        Assert.fail();
    }

    //PIT: the SequenceGroup inner class has a problem... the sequencePeriod field can't be different to 0. There is only one possibility
    // when the SequenceGroup is deserialized.
    @Test
    public void testConstructor3() {

        long[] bookieContent = {3, 4, 5, 10, 11, 12, 13, 5, 6, 7, 8};
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(bookieContent);

        Assert.assertEquals(bookieContent.length, this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

        for (long entryId: bookieContent) {
            if(entryId >= 0)
                Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
        }

        byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();


        serializedState[63 + 24] = 1;   //set the "sequencePeriod" field to 1
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(serializedState);

        for(long entryID: bookieContent) {
            this.availabilityOfEntriesOfLedger.isEntryAvailable(entryID);
        }
    }

}
