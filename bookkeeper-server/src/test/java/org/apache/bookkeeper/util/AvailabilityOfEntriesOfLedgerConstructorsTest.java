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
    private final boolean isExpectedAnException;

    @Parameterized.Parameters
    public static Collection<Object[]> data(){

        long[] bookieContent = new long[]{1L, 3L, 4L};
        long[] bookieContentWithSequencePeriod = new long[]{1L, 3L, 5L, 7L};
        long[] bookieContent3 = {3, 4, 5, 5, 10, 11, 12, 13, 6, 7, 8};
        long[] bookieContent4 = {3, 4, 5, -1, 0, 1, 6, 7, 8};

        long[] invalidBookieContent = {-1, 0, 1, 2, 3};


        return Arrays.asList(new Object[][]{
                {bookieContent, false},
                {new long[]{}, false},
                {null, true},
                {invalidBookieContent, false},

                //JACOCO
                {bookieContentWithSequencePeriod, false},
                {bookieContent3, false},
                {bookieContent4, false}
        });
    }

    public AvailabilityOfEntriesOfLedgerConstructorsTest(long[] bookieContent, boolean isExpectedAnException){
        this.bookieContent = bookieContent;
        this.isExpectedAnException = isExpectedAnException;
    }

    public static int countElem(long[] content) {
        List<Long> longList = new ArrayList<>();
        PrimitiveIterator.OfLong iterator = Arrays.stream(content).iterator();

        while(iterator.hasNext()) {
            Long value = iterator.next();
            if(!longList.contains(value) && value >= 0) {
                longList.add(value);
            }
        }

        return longList.size();
    }

    @Test
    public void testConstructor1() {
        try {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.bookieContent);

            Assert.assertEquals(countElem(bookieContent), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                if(entryId >= 0)
                    Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }
        } catch (Exception e) {
            if(isExpectedAnException) return;
            Assert.fail();
        }
        if(isExpectedAnException) Assert.fail();
    }

    @Test
    public void testConstructor2() {
        try {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.bookieContent);

            Assert.assertEquals(countElem(bookieContent), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                if(entryId >= 0)
                    Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }

            byte[] serializedState = availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();

            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(serializedState);

            Assert.assertEquals(countElem(bookieContent), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                if(entryId >= 0)
                    Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }
        } catch (Exception e) {
            if(isExpectedAnException) return;
            Assert.fail();
        }
        if(isExpectedAnException) Assert.fail();
    }

    @Test
    public void testConstructor3() {
        try {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.bookieContent);

            ByteBuf byteBuf = Unpooled.buffer();
            byteBuf.writeBytes(availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger());
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);

            Assert.assertEquals(countElem(bookieContent), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());

            for (long entryId : this.bookieContent) {
                if(entryId >= 0)
                    Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entryId));
            }
        } catch (Exception e) {
            if(isExpectedAnException) return;
            Assert.fail();
        }
        if(isExpectedAnException) Assert.fail();
    }
}
