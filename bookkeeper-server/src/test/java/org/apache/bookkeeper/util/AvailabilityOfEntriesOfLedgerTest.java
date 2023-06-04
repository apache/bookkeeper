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

    @Parameterized.Parameters
    public static Collection<Object[]> data(){

        return Arrays.asList(new Object[][]{
                {-1L, false},
                {0, false},
                {1, true}
        });
    }

    public AvailabilityOfEntriesOfLedgerTest(long entryId, boolean expectedResult){

        this.entryId = entryId;
        this.expectedResult = expectedResult;

        long[] content = {1, 7, 9}; //entries contained in the object
        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(content).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);

    }

    @Test
    public void testIsEntryAvailable() {
        boolean actual = this.availabilityOfEntriesOfLedger.isEntryAvailable(this.entryId);
        Assert.assertEquals(this.expectedResult, actual);
    }

    @Test
    public void testGetTotalNumOfAvailableEntries() {

    }
}