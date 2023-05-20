package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class BookieImplAddressTest {


    private BookieSocketAddress bookieSocketAddress = null;
    private ServerConfiguration conf;

    private ExpectedValue expectedValue;

    private final String HOST_NAME = "filippo-VirtualBox";

    public BookieImplAddressTest(String address, int port, String interfaceName, boolean hostAsName, boolean shortName, boolean loopback, ExpectedValue expectedValue) {
        conf = new ServerConfiguration();
        conf.setAdvertisedAddress(address);
        conf.setBookiePort(port);
        conf.setListeningInterface(interfaceName); //enp0s3 enp0s8 enp0s9 enp0s810
        conf.setUseHostNameAsBookieID(hostAsName);
        conf.setUseShortHostName(shortName);
        conf.setAllowLoopback(loopback);
        this.expectedValue = expectedValue;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParams() {
        return Arrays.asList(new Object[][] {
                //address    //port    //interface      //hostNameAsBookie    //shortName    //allowLoopBack    //expectedValue
                {"",           1,       null,            false,                false,         true,            ExpectedValue.PASSED},
                {"",           1,     "notAnInterface",  false,                false,         true,            ExpectedValue.UH_EXCEPTION},
                {"",           0,     "enp0s9",           true,                false,         true,            ExpectedValue.PASSED},
                {"",           0,     "enp0s9",          false,                false,         false,           ExpectedValue.UH_EXCEPTION},
                {"",           1,     "",                false,                false,         true,            ExpectedValue.UH_EXCEPTION},
         {"192.168.56.102",    1,     "enp0s9",          true,                 true,          true,            ExpectedValue.PASSED },
         {"192.168.56.102",    1,     "enp0s9",          true,                 true,          false,           ExpectedValue.PASSED},
                {null,         1,     "enp0s9",          false,                false,         true,            ExpectedValue.PASSED},
                {"",           -1,    "enp0s9",          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
                {"",         65536,   "enp0s9",          true,                 false,         true,            ExpectedValue.IA_EXCEPTION},
         {"300.598.1.2",      1,      "enp0s9",          false,                false,         true,            ExpectedValue.PASSED}


        });
    }

    @Test
    public void getAddressTest() {
        ExpectedValue actualValue = null;

        try {
            bookieSocketAddress = BookieImpl.getBookieAddress(conf);
            Assert.assertEquals("La porta passata come parametro è diversa da quella effettiva del bookieSocketAddress", conf.getBookiePort(), bookieSocketAddress.getPort());
            if (conf.getAdvertisedAddress() == null && bookieSocketAddress.getHostName().equals("127.0.1.1")) {
                actualValue = ExpectedValue.PASSED;
            } else if (conf.getAdvertisedAddress().equals("") && bookieSocketAddress.getHostName().equals("filippo-VirtualBox")) {
                actualValue = ExpectedValue.PASSED;

            } else if (conf.getAdvertisedAddress().equals(bookieSocketAddress.getHostName())) {
                actualValue = ExpectedValue.PASSED;
            } else if (bookieSocketAddress.getHostName().equals("127.0.1.1")) {
                actualValue = ExpectedValue.PASSED;
            }


        } catch (UnknownHostException e) {
            actualValue = ExpectedValue.UH_EXCEPTION;
            System.out.println(e.toString());
        } catch (IllegalArgumentException e) {
            actualValue = ExpectedValue.IA_EXCEPTION;
            System.out.println(e.toString());

        }
        Assert.assertEquals(expectedValue, actualValue);
    }


    private enum ExpectedValue {
        PASSED, UH_EXCEPTION, IA_EXCEPTION
    }


}