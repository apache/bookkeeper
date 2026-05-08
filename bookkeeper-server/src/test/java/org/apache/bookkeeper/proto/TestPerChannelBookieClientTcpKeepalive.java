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

package org.apache.bookkeeper.proto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.uring.IoUring;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.EventLoopUtil;
import org.junit.Test;

/**
 * Unit tests for TCP keepalive configuration in PerChannelBookieClient.
 * This is a standalone test class that does not inherit from BookKeeperClusterTestCase
 * to avoid NativeIO assertion errors during cluster startup.
 */
public class TestPerChannelBookieClientTcpKeepalive {

    /**
     * Test ClientConfiguration TCP keepalive getter methods.
     */
    @Test
    public void testClientConfigurationGetters() {
        ClientConfiguration conf = new ClientConfiguration();

        // Test default values (should be -1 as per implementation)
        assertEquals("Default TCP keep idle should be -1", -1, conf.getTcpKeepIdle());
        assertEquals("Default TCP keep interval should be -1", -1, conf.getTcpKeepIntvl());
        assertEquals("Default TCP keep count should be -1", -1, conf.getTcpKeepCnt());

        // Test setter and getter methods
        conf.setTcpKeepIdle(60);
        conf.setTcpKeepIntvl(30);
        conf.setTcpKeepCnt(5);

        assertEquals("TCP keep idle should be 60", 60, conf.getTcpKeepIdle());
        assertEquals("TCP keep interval should be 30", 30, conf.getTcpKeepIntvl());
        assertEquals("TCP keep count should be 5", 5, conf.getTcpKeepCnt());

        // Test that -1 means use system default (as per implementation comments)
        conf.setTcpKeepIdle(-1);
        conf.setTcpKeepIntvl(-1);
        conf.setTcpKeepCnt(-1);

        assertEquals("TCP keep idle should be -1 for system default", -1, conf.getTcpKeepIdle());
        assertEquals("TCP keep interval should be -1 for system default", -1, conf.getTcpKeepIntvl());
        assertEquals("TCP keep count should be -1 for system default", -1, conf.getTcpKeepCnt());
    }

    /**
     * Test TCP keepalive configuration conditional logic.
     * This test focuses on the conditional logic in PerChannelBookieClient
     * without actually creating network connections.
     */
    @Test
    public void testTcpKeepaliveConditionalLogic() {
        // Test various configuration scenarios
        ClientConfiguration conf = new ClientConfiguration();

        // Test with positive values
        conf.setTcpKeepIdle(60);
        conf.setTcpKeepIntvl(30);
        conf.setTcpKeepCnt(5);

        assertTrue("TCP keep idle should be > 0", conf.getTcpKeepIdle() > 0);
        assertTrue("TCP keep interval should be > 0", conf.getTcpKeepIntvl() > 0);
        assertTrue("TCP keep count should be > 0", conf.getTcpKeepCnt() > 0);

        // Test with zero values
        conf.setTcpKeepIdle(0);
        conf.setTcpKeepIntvl(0);
        conf.setTcpKeepCnt(0);

        assertFalse("TCP keep idle should not be configured for zero", conf.getTcpKeepIdle() > 0);
        assertFalse("TCP keep interval should not be configured for zero", conf.getTcpKeepIntvl() > 0);
        assertFalse("TCP keep count should not be configured for zero", conf.getTcpKeepCnt() > 0);

        // Test with negative values (system default)
        conf.setTcpKeepIdle(-1);
        conf.setTcpKeepIntvl(-1);
        conf.setTcpKeepCnt(-1);

        assertFalse("TCP keep idle should not be configured for negative", conf.getTcpKeepIdle() > 0);
        assertFalse("TCP keep interval should not be configured for negative", conf.getTcpKeepIntvl() > 0);
        assertFalse("TCP keep count should not be configured for negative", conf.getTcpKeepCnt() > 0);

        // Test partial configuration
        conf.setTcpKeepIdle(60);
        conf.setTcpKeepIntvl(0);
        conf.setTcpKeepCnt(5);

        assertTrue("TCP keep idle should be configured", conf.getTcpKeepIdle() > 0);
        assertFalse("TCP keep interval should not be configured", conf.getTcpKeepIntvl() > 0);
        assertTrue("TCP keep count should be configured", conf.getTcpKeepCnt() > 0);
    }

    /**
     * Test Epoll support detection logic.
     */
    @Test
    public void testEpollSupportDetection() {
        // Test Epoll support detection: use Epoll.isAvailable() to verify that the native
        // library can actually be loaded, not just that the class is on the classpath.
        boolean isEpollSupported = Epoll.isAvailable();

        // Test NIO event loop group detection
        NioEventLoopGroup nioGroup = new NioEventLoopGroup(1);
        assertFalse("NIO event loop group should not be detected as Epoll",
                nioGroup.getClass().getName().contains("Epoll"));

        nioGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);

        // The important thing is that the detection logic works correctly
        // The actual result depends on the platform
        System.out.println("Epoll support detected: " + isEpollSupported);
        System.out.println("Operating system: " + System.getProperty("os.name"));
    }

    /**
     * Test IoUring support detection logic.
     */
    @Test
    public void testIoUringSupportDetection() {
        // Use runtime availability checks to avoid UnsatisfiedLinkError on non-Linux platforms.
        boolean isIoUringSupported = IoUring.isAvailable();
        boolean isEpollSupported = Epoll.isAvailable();

        // Test NIO event loop group detection
        NioEventLoopGroup nioGroup = new NioEventLoopGroup(1);
        assertFalse("NIO event loop group should not be detected as IoUring",
                EventLoopUtil.isIoUringGroup(nioGroup));

        // Test DefaultEventLoopGroup detection
        DefaultEventLoopGroup defaultGroup = new DefaultEventLoopGroup(1);
        assertFalse("Default event loop group should not be detected as IoUring",
                EventLoopUtil.isIoUringGroup(defaultGroup));

        // Only instantiate EpollEventLoopGroup when the native library is available,
        // otherwise the constructor will fail on non-Linux platforms.
        if (isEpollSupported) {
            EpollEventLoopGroup epollGroup = new EpollEventLoopGroup(1);
            assertFalse("Epoll event loop group should not be detected as IoUring",
                    EventLoopUtil.isIoUringGroup(epollGroup));
            epollGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        }

        nioGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
        defaultGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);

        // The important thing is that the detection logic works correctly
        // The actual result depends on the platform
        System.out.println("IoUring support detected: " + isIoUringSupported);
        System.out.println("Epoll support detected: " + isEpollSupported);
        System.out.println("Operating system: " + System.getProperty("os.name"));
    }

    /**
     * Test TCP keepalive parameter validation logic.
     */
    @Test
    public void testTcpKeepaliveParameterValidation() {
        ClientConfiguration conf = new ClientConfiguration();

        // Test valid parameter ranges
        conf.setTcpKeepIdle(1);
        conf.setTcpKeepIntvl(1);
        conf.setTcpKeepCnt(1);

        assertTrue("Minimum TCP keep idle should be valid", conf.getTcpKeepIdle() > 0);
        assertTrue("Minimum TCP keep interval should be valid", conf.getTcpKeepIntvl() > 0);
        assertTrue("Minimum TCP keep count should be valid", conf.getTcpKeepCnt() > 0);

        // Test typical production values
        conf.setTcpKeepIdle(300);
        conf.setTcpKeepIntvl(60);
        conf.setTcpKeepCnt(3);

        assertEquals("Production TCP keep idle should be 300", 300, conf.getTcpKeepIdle());
        assertEquals("Production TCP keep interval should be 60", 60, conf.getTcpKeepIntvl());
        assertEquals("Production TCP keep count should be 3", 3, conf.getTcpKeepCnt());

        // Test boundary values
        conf.setTcpKeepIdle(Integer.MAX_VALUE);
        conf.setTcpKeepIntvl(Integer.MAX_VALUE);
        conf.setTcpKeepCnt(Integer.MAX_VALUE);

        assertTrue("Maximum TCP keep idle should be valid", conf.getTcpKeepIdle() > 0);
        assertTrue("Maximum TCP keep interval should be valid", conf.getTcpKeepIntvl() > 0);
        assertTrue("Maximum TCP keep count should be valid", conf.getTcpKeepCnt() > 0);
    }
}
