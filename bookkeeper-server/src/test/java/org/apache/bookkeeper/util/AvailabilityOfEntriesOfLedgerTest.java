package org.apache.bookkeeper.util;


import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class AvailabilityOfEntriesOfLedgerTest {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger; // tested object
    private long entryId;
    private boolean expectedResult;
    private long[] bookieContent;

    @Parameterized.Parameters
    public static Collection<Object[]> data(){

        long maxValue = 4L;
        long[] bookieContent = new long[]{1L, 3L, 4L};
        long[] bookieContent2 = new long[]{1L, 3L, 5L, 7L, 9L};

        return Arrays.asList(new Object[][]{
                {-1L, bookieContent, false},
                {0, bookieContent, false},
                {1, bookieContent, true},
                {maxValue, bookieContent, true},
                {maxValue + 1, bookieContent, false},

                //JACOCO
                {9, bookieContent2, true},
        });
    }

    public AvailabilityOfEntriesOfLedgerTest(long entryId, long[] bookieContent, boolean expectedResult){

        this.entryId = entryId;
        this.bookieContent = bookieContent;
        this.expectedResult = expectedResult;

        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(this.bookieContent).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);
    }

    @Test
    public void testIsEntryAvailable() {
        boolean result = this.availabilityOfEntriesOfLedger.isEntryAvailable(this.entryId);
        Assert.assertEquals(this.expectedResult, result);
    }
}