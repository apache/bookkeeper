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

    public AvailabilityOfEntriesOfLedgerTest(long entryId, boolean expectedResult){

        this.entryId = entryId;
        this.expectedResult = expectedResult;

        long[] content = {1, 3, 4}; //entries contained in the object
        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(content).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);
    }

    @Test
    public void testIsEntryAvailable() {
        boolean actual = this.availabilityOfEntriesOfLedger.isEntryAvailable(this.entryId);
        Assert.assertEquals(this.expectedResult, actual);

        //Non viene testata la parte in cui viene lanciata l'eccezione: come faccio a creare un oggetto non chiuso?
    }
}