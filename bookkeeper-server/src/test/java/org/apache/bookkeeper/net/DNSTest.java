/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.net;

import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;
import javax.naming.NamingException;
import lombok.Cleanup;
import org.junit.Test;
import org.mockito.MockedStatic;

public class DNSTest {

    private NetworkInterface getLoopbackInterface() throws SocketException {
        Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface nif : Collections.list(nifs)) {
            if (nif.isLoopback()) {
                return nif;
            }
        }

        return null;
    }

    @Test
    public void testGetHostsByLoopbackInterface() throws UnknownHostException, SocketException {
        NetworkInterface loopbackInterface = getLoopbackInterface();
        if (loopbackInterface == null) {
            return;
        }

        String loopbackInterfaceName = loopbackInterface.getName();

        @Cleanup
        MockedStatic<DNS> dnsMockedStatic = mockStatic(DNS.class, CALLS_REAL_METHODS);
        DNS.getHosts(loopbackInterfaceName, null);

        dnsMockedStatic.verify(() -> DNS.reverseDns(any(), any()), never());
    }


    @Test
    public void testReversDNSByLoopbackInterface() throws UnknownHostException, SocketException, NamingException {
        NetworkInterface loopbackInterface = getLoopbackInterface();
        if (loopbackInterface == null) {
            return;
        }

        String[] iPs = DNS.getIPs(loopbackInterface.getName());
        for (String iP : iPs) {
            InetAddress inetAddress = InetAddress.getByName(iP);
            if (inetAddress.isLoopbackAddress()) {
                String host = DNS.reverseDns(inetAddress, null);
                assertNotEquals(host.charAt(host.length() - 1), '.');
            }
        }
    }
}
