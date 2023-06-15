package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TestAvailabilityOfEntriesOfLedgerConstructors {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger; // tested object

    //Ba-dua: non è coperto il caso in cui l'header per la deserializzazione non è valido, quindi questo test copre questa situazione.
    @Test
    public void testConstructor4() {
        try {

            ByteBuf byteBuf = Unpooled.buffer();
            for(int i = 0; i < 64; i++)
                byteBuf.writeByte(1);
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(byteBuf);

        }  catch (IllegalArgumentException e) {
            return;
        }
        Assert.fail();
    }
}
