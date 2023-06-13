/*
package org.apache.bookkeeper.util;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.PrimitiveIterator;

@RunWith(Parameterized.class)
public class AvailabilityOfEntriesOfLedgerConstructorsTest {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger; // tested object
    private long entryId;
    private boolean expectedResult;

    @Parameterized.Parameters
    public static Collection<Object[]> data(){

        long maxValue = 4L;

        return Arrays.asList(new Object[][]{
                {-1L, false},
                {0, false},
                {1, true},
                {maxValue - 1L, true},
                {maxValue, true},
                {maxValue + 1, false},
        });
    }

    public AvailabilityOfEntriesOfLedgerConstructorsTest(long entryId, boolean expectedResult){

        this.entryId = entryId;
        this.expectedResult = expectedResult;

        long[] content = {1, 3, 4}; //entries contained in the object
        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(content).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);
    }

    @Test
    public void testConstructor1() {
        long[] content = {1, 3, 4}; //entries contained in the object
        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(content).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);

        Assert.assertEquals(this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(), 3);

        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(1));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(3));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(4));
        Assert.assertFalse(this.availabilityOfEntriesOfLedger.isEntryAvailable(7));

    }

    @Test
    public void testConstructor2() {
        long[] content = {1, 3, 4}; //entries contained in the object
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(content);

        Assert.assertEquals(this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(), 3);

        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(1));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(3));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(4));
        Assert.assertFalse(this.availabilityOfEntriesOfLedger.isEntryAvailable(7));

    }

    @Test
    public void testConstructor3() {
        long[] content = {1, 3, 4}; //entries contained in the object
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(content);

        Assert.assertEquals(this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(), 3);

        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(1));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(3));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(4));
        Assert.assertFalse(this.availabilityOfEntriesOfLedger.isEntryAvailable(7));

        byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();

        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(serializedState);


        Assert.assertEquals(this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(), 3);

        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(1));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(3));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(4));
        Assert.assertFalse(this.availabilityOfEntriesOfLedger.isEntryAvailable(7));
    }

    @Test
    public void testConstructor4() {
        try {
            ByteBuf byteBuf = Unpooled.EMPTY_BUFFER;
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);
        } catch (Exception e) {
            return;
        }
        Assert.fail();
    }

    @Test
    public void testConstructor5() {
        long[] content = {1, 3, 4}; //entries contained in the object
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(content);

        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger());
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);

        Assert.assertEquals(this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries(), 3);

        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(1));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(3));
        Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(4));
        Assert.assertFalse(this.availabilityOfEntriesOfLedger.isEntryAvailable(7));

    }
}*/
