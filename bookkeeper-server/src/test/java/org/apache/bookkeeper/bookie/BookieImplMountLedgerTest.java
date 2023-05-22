package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.bookie.util.TestBKConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookieImplMountLedgerTest {

    private LedgerStorage ledgerStorage;
    private ServerConfiguration serverConfiguration;
    private ExpectedValue expectedValue;
    private ExpectedValue actualValue;

    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][] {

                   //LeadgerStorage        UsageThreshold         WarnThreshold       ExpectedValue
                {LedgerStorageType.VALID,       0.95f,                0.90f,            ExpectedValue.PASSED },
                {LedgerStorageType.VALID,       0.20f,                0.90f,            ExpectedValue.SOME_EXCEPTION }

        });
    }

    public BookieImplMountLedgerTest( LedgerStorageType type, float usageThreshold, float warnThreshold, ExpectedValue expectedValue) {
        //TODO TYPE LS
        this.ledgerStorage = new SortedLedgerStorage(); //è quello di default secondo la documentazione, magari si può provare anche con gli altri valori TODO
        this.serverConfiguration = TestBKConfiguration.newServerConfiguration();
        serverConfiguration.setDiskUsageThreshold(usageThreshold);
        serverConfiguration.setDiskUsageWarnThreshold(warnThreshold);
        this.expectedValue = expectedValue;
    }

    @Test
    public void mountLedgerTest() {

        actualValue = ExpectedValue.PASSED;

        try {
            BookieImpl.mountLedgerStorageOffline(serverConfiguration, ledgerStorage);
        } catch (Exception e) {
            e.printStackTrace();
            actualValue = ExpectedValue.SOME_EXCEPTION;

        }

        Assert.assertEquals(expectedValue, actualValue);


    }

    /*
     * Si può provare con il leadger storage null
     *
     * Si possono provare diverse combinazioni per diskusagetreshold e diskusagewarn nel ServerConfiguration
     */

}

enum LedgerStorageType {
    VALID, INVALID, NULL
}

enum ExpectedValue {
    PASSED, SOME_EXCEPTION
}
