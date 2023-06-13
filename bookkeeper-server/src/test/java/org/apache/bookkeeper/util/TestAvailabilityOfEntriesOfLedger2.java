package org.apache.bookkeeper.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestAvailabilityOfEntriesOfLedger2 {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;    //Object under test
    private long startEntry;    //ID of the entry from which to search
    private long lastEntry;     //ID ot the latest entry
    private BitSet bookieEntries;   //Entries in bookie
    private List<Long> expectedResult;  //Expected result


    @Parameterized.Parameters
    public static Collection<Object[]> data(){

        long lastBookieEntry = 15L;
        List<Long> exception = new ArrayList<>();
        exception.add(-1L);

        //BitSet valido
        BitSet validBitSet = new BitSet((int) lastBookieEntry);
        long[] bookieEntries1 = {3, 5, 6};
        List<Long> bookieEntriesList1 = new ArrayList<>();
        bookieEntriesList1.add(3L);
        bookieEntriesList1.add(5L);
        bookieEntriesList1.add(6L);


        for(int i = 1; i < lastBookieEntry; i = i + 2){                          // validBitSet = [1,3,5,7,9,11,13]
            validBitSet.set(i);
        }


        List<Long> unavailableEntries1 = new ArrayList<>();
        unavailableEntries1.add(1L);
        unavailableEntries1.add(7L);
        unavailableEntries1.add(9L);
        unavailableEntries1.add(11L);
        unavailableEntries1.add(13L);

        //BitSet Vuoto e con size ZERO
        BitSet emptyBitSet = new BitSet(0);

        //BitSet senza matching value nel bookie
        BitSet bitSetWithoutMatchingValues = new BitSet(30);
        bitSetWithoutMatchingValues.flip(1, 2);

        List<Long> unavailableEntries2 = new ArrayList<>();
        for(long i = -1; i < lastBookieEntry + 1; i++) {
            if(!bookieEntriesList1.contains(i)) {
                unavailableEntries2.add(i);
            }
        }

        //BitSet con valori impostati oltre il range [startEntryId, lastEntryId]
        BitSet excedingBitset = new BitSet((int)lastBookieEntry+10);   // sono presenti tutte le entry
        excedingBitset.flip(0, (int)lastBookieEntry+11);   // l'ultimo indice è esclusivo


        long[] bookieEntries2 = {3, 5, 7};
        List<Long> unavailableEntries3 = new ArrayList<>();
        unavailableEntries3.add(1L);
        unavailableEntries3.add(9L);
        unavailableEntries3.add(11L);
        unavailableEntries3.add(13L);


        return Arrays.asList(new Object[][]{
                {0, lastBookieEntry, validBitSet, bookieEntries1, unavailableEntries1},
                {lastBookieEntry, lastBookieEntry, bitSetWithoutMatchingValues, bookieEntries1, new ArrayList<>()},
                {lastBookieEntry, lastBookieEntry + 1, null, bookieEntries1, exception},
                {-1, lastBookieEntry, excedingBitset, bookieEntries1, unavailableEntries2},
                {1, 3, emptyBitSet, bookieEntries1, new ArrayList<>()},
                {lastBookieEntry, lastBookieEntry-1, validBitSet, bookieEntries1, new ArrayList<>()},

                //JACOCO
                {0, lastBookieEntry, validBitSet, bookieEntries2, unavailableEntries3}
            }
        );
    }

    public TestAvailabilityOfEntriesOfLedger2(long startEntryId, long lastEntryId, BitSet expectedEntries,
                                              long[] bookieEntries, List<Long> expectedResult){

        this.startEntry = startEntryId;
        this.lastEntry = lastEntryId;
        this.expectedResult = new ArrayList<>();
        this.expectedResult.addAll(expectedResult);
        this.bookieEntries = expectedEntries;

        PrimitiveIterator.OfLong primitiveIterator = Arrays.stream(bookieEntries).iterator();
        this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(primitiveIterator);

    }

    private static List<Long> entryBetween(List<Long> unavailable, long startEntryId, long lastEntryId){
        ArrayList<Long> filter = new ArrayList<>();

        for(Long l: unavailable){
            if(l>=startEntryId || l<=lastEntryId)
                filter.add(l);
        }
        return filter;

    }

    @Test
    public void testAvailabilityOfEntries(){
        List<Long> result;
        try {
            result = this.availabilityOfEntriesOfLedger.getUnavailableEntries(this.startEntry, this.lastEntry, this.bookieEntries);
        } catch (NullPointerException e){
            result = new ArrayList<>();
            result.add(-1L);
        }

        System.out.println(result);
        assertEquals(this.expectedResult, result);

    }
}
