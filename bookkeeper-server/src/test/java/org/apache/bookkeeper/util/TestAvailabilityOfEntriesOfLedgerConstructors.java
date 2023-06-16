package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

public class TestAvailabilityOfEntriesOfLedgerConstructors {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger; // tested object

    //Ba-dua: non è coperto il caso in cui l'header per la deserializzazione non è valido, quindi questo test copre questa situazione.
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
}
