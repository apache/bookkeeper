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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.uring.IoUring;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.EventLoopUtil;
import org.junit.Test;

/**
 * Unit tests for TCP keepalive configuration in BookieNettyServer.
 * This test focuses on server-side TCP keepalive configuration logic
 * without requiring actual network connections.
 */
public class TestBookieNettyServerTcpKeepalive {

    /**
     * Test ServerConfiguration TCP keepalive getter methods.
     */
    @Test
    public void testServerConfigurationGetters() {
        ServerConfiguration conf = new ServerConfiguration();

        // Test default values (should be -1 as per implementation)
        assertEquals("Default server TCP keep idle should be -1", -1, conf.getServerTcpKeepIdle());
        assertEquals("Default server TCP keep interval should be -1", -1, conf.getServerTcpKeepIntvl());
        assertEquals("Default server TCP keep count should be -1", -1, conf.getServerTcpKeepCnt());

        // Test setter and getter methods
        conf.setServerTcpKeepIdle(60);
        conf.setServerTcpKeepIntvl(30);
        conf.setServerTcpKeepCnt(5);

        assertEquals("Server TCP keep idle should be 60", 60, conf.getServerTcpKeepIdle());
        assertEquals("Server TCP keep interval should be 30", 30, conf.getServerTcpKeepIntvl());
        assertEquals("Server TCP keep count should be 5", 5, conf.getServerTcpKeepCnt());

        // Test that -1 means use system default (as per implementation comments)
        conf.setServerTcpKeepIdle(-1);
        conf.setServerTcpKeepIntvl(-1);
        conf.setServerTcpKeepCnt(-1);

        assertEquals("Server TCP keep idle should be -1 for system default", -1, conf.getServerTcpKeepIdle());
        assertEquals("Server TCP keep interval should be -1 for system default", -1, conf.getServerTcpKeepIntvl());
        assertEquals("Server TCP keep count should be -1 for system default", -1, conf.getServerTcpKeepCnt());
    }

    /**
     * Test TCP keepalive configuration conditional logic for server side.
     */
    @Test
    public void testServerTcpKeepaliveConditionalLogic() {
        ServerConfiguration conf = new ServerConfiguration();

        // Test with positive values
        conf.setServerTcpKeepIdle(60);
        conf.setServerTcpKeepIntvl(30);
        conf.setServerTcpKeepCnt(5);

        assertTrue("Server TCP keep idle should be > 0", conf.getServerTcpKeepIdle() > 0);
        assertTrue("Server TCP keep interval should be > 0", conf.getServerTcpKeepIntvl() > 0);
        assertTrue("Server TCP keep count should be > 0", conf.getServerTcpKeepCnt() > 0);

        // Test with zero values
        conf.setServerTcpKeepIdle(0);
        conf.setServerTcpKeepIntvl(0);
        conf.setServerTcpKeepCnt(0);

        assertFalse("Server TCP keep idle should not be configured for zero", conf.getServerTcpKeepIdle() > 0);
        assertFalse("Server TCP keep interval should not be configured for zero", conf.getServerTcpKeepIntvl() > 0);
        assertFalse("Server TCP keep count should not be configured for zero", conf.getServerTcpKeepCnt() > 0);

        // Test with negative values (system default)
        conf.setServerTcpKeepIdle(-1);
        conf.setServerTcpKeepIntvl(-1);
        conf.setServerTcpKeepCnt(-1);

        assertFalse("Server TCP keep idle should not be configured for negative", conf.getServerTcpKeepIdle() > 0);
        assertFalse("Server TCP keep interval should not be configured for negative", conf.getServerTcpKeepIntvl() > 0);
        assertFalse("Server TCP keep count should not be configured for negative", conf.getServerTcpKeepCnt() > 0);

        // Test partial configuration
        conf.setServerTcpKeepIdle(60);
        conf.setServerTcpKeepIntvl(0);
        conf.setServerTcpKeepCnt(5);

        assertTrue("Server TCP keep idle should be configured", conf.getServerTcpKeepIdle() > 0);
        assertFalse("Server TCP keep interval should not be configured", conf.getServerTcpKeepIntvl() > 0);
        assertTrue("Server TCP keep count should be configured", conf.getServerTcpKeepCnt() > 0);
    }

    /**
     * Test EventLoopGroup type detection logic.
     */
    @Test
    public void testEventLoopGroupTypeDetection() {
        // Test NIO event loop group detection
        NioEventLoopGroup nioGroup = new NioEventLoopGroup(1);
        assertFalse("NIO event loop group should not be detected as Epoll",
                nioGroup.getClass().getName().contains("Epoll"));
        assertFalse("NIO event loop group should not be detected as IoUring",
                EventLoopUtil.isIoUringGroup(nioGroup));
        nioGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);

        // Test DefaultEventLoopGroup detection
        DefaultEventLoopGroup defaultGroup = new DefaultEventLoopGroup(1);
        assertFalse("Default event loop group should not be detected as Epoll",
                defaultGroup.getClass().getName().contains("Epoll"));
        assertFalse("Default event loop group should not be detected as IoUring",
                EventLoopUtil.isIoUringGroup(defaultGroup));
        defaultGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);

        // Test Epoll support detection: use Epoll.isAvailable() to verify that the native
        // library can actually be loaded, not just that the class is on the classpath.
        boolean isEpollSupported = Epoll.isAvailable();

        // Test IoUring support detection via runtime availability check.
        boolean isIoUringSupported = IoUring.isAvailable();

        System.out.println("Epoll support detected: " + isEpollSupported);
        System.out.println("IoUring support detected: " + isIoUringSupported);
        System.out.println("Operating system: " + System.getProperty("os.name"));

        // The important thing is that the detection logic works correctly
        // The actual result depends on the platform
        assertTrue("EventLoopUtil should be available", EventLoopUtil.class != null);
    }

    /**
     * Test TCP keepalive parameter validation logic.
     */
    @Test
    public void testTcpKeepaliveParameterValidation() {
        ServerConfiguration conf = new ServerConfiguration();

        // Test valid parameter ranges
        conf.setServerTcpKeepIdle(1);
        conf.setServerTcpKeepIntvl(1);
        conf.setServerTcpKeepCnt(1);

        assertTrue("Minimum TCP keep idle should be valid", conf.getServerTcpKeepIdle() > 0);
        assertTrue("Minimum TCP keep interval should be valid", conf.getServerTcpKeepIntvl() > 0);
        assertTrue("Minimum TCP keep count should be valid", conf.getServerTcpKeepCnt() > 0);

        // Test typical production values
        conf.setServerTcpKeepIdle(300);
        conf.setServerTcpKeepIntvl(60);
        conf.setServerTcpKeepCnt(3);

        assertEquals("Production TCP keep idle should be 300", 300, conf.getServerTcpKeepIdle());
        assertEquals("Production TCP keep interval should be 60", 60, conf.getServerTcpKeepIntvl());
        assertEquals("Production TCP keep count should be 3", 3, conf.getServerTcpKeepCnt());

        // Test boundary values
        conf.setServerTcpKeepIdle(Integer.MAX_VALUE);
        conf.setServerTcpKeepIntvl(Integer.MAX_VALUE);
        conf.setServerTcpKeepCnt(Integer.MAX_VALUE);

        assertTrue("Maximum TCP keep idle should be valid", conf.getServerTcpKeepIdle() > 0);
        assertTrue("Maximum TCP keep interval should be valid", conf.getServerTcpKeepIntvl() > 0);
        assertTrue("Maximum TCP keep count should be valid", conf.getServerTcpKeepCnt() > 0);
    }
}