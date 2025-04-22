/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.apache.bookkeeper.bookie.BookieImpl.getBookieAddress;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.junit.Test;

public class BookieSocketAddressTest {

    private void testAdvertisedWithLoopbackAddress(String address) throws UnknownHostException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAdvertisedAddress(address);
        conf.setAllowLoopback(false);
        assertThatThrownBy(() -> getBookieAddress(conf)).isExactlyInstanceOf(UnknownHostException.class);

        conf.setAllowLoopback(true);
        BookieSocketAddress bookieAddress = getBookieAddress(conf);
        assertThat(bookieAddress.getHostName()).isEqualTo(address);
    }

    @Test
    public void testAdvertisedWithLoopbackAddress() throws UnknownHostException {
        testAdvertisedWithLoopbackAddress("localhost");
        testAdvertisedWithLoopbackAddress("127.0.0.1");
    }

    @Test
    public void testAdvertisedWithNonLoopbackAddress() throws UnknownHostException {
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        if (hostAddress == null) {
            throw new UnknownHostException("Host address is null");
        }
        ServerConfiguration conf = new ServerConfiguration();
        conf.setAllowLoopback(false);
        conf.setAdvertisedAddress(hostAddress);
        BookieSocketAddress bookieAddress = getBookieAddress(conf);
        assertThat(bookieAddress.getHostName()).isEqualTo(hostAddress);
    }

    @Test
    public void testBookieAddressIsIPAddressByDefault() throws UnknownHostException {
        ServerConfiguration conf = new ServerConfiguration();
        BookieSocketAddress bookieAddress = getBookieAddress(conf);
        assertThat(InetAddresses.isInetAddress(bookieAddress.getHostName())).isTrue();
    }

    @Test
    public void testBookieAddressIsHostname() throws UnknownHostException {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setUseHostNameAsBookieID(true);
        BookieSocketAddress bookieAddress = getBookieAddress(conf);
        assertThat(InetAddresses.isInetAddress(bookieAddress.getHostName())).isFalse();
    }
}
