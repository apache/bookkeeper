/**
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
package org.apache.distributedlog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.LogWriter;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.distributedlog.callback.NamespaceListener;
import org.apache.distributedlog.exceptions.AlreadyClosedException;
import org.apache.distributedlog.exceptions.InvalidStreamNameException;
import org.apache.distributedlog.exceptions.LockingException;
import org.apache.distributedlog.exceptions.ZKException;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.util.DLUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Test Cases for {@link Namespace}.
 */
public class TestBKDistributedLogNamespace extends TestDistributedLogBase {

    @Rule
    public TestName runtime = new TestName();

    static final Logger LOG = LoggerFactory.getLogger(TestBKDistributedLogNamespace.class);

    protected static DistributedLogConfiguration conf =
            new DistributedLogConfiguration().setLockTimeout(10)
                .setEnableLedgerAllocatorPool(true).setLedgerAllocatorPoolName("test");

    private ZooKeeperClient zooKeeperClient;

    @Before
    public void setup() throws Exception {
        zooKeeperClient =
            TestZooKeeperClientBuilder.newBuilder()
                .uri(createDLMURI("/"))
                .build();
    }

    @After
    public void teardown() throws Exception {
        zooKeeperClient.close();
    }

    @Test(timeout = 60000)
    public void testCreateLogPath0() throws Exception {
        createLogPathTest("/create/log/path/" + runtime.getMethodName());
    }

    @Test(timeout = 60000)
    public void testCreateLogPath1() throws Exception {
        createLogPathTest("create/log/path/" + runtime.getMethodName());
    }

    private void createLogPathTest(String logName) throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        ensureURICreated(zooKeeperClient.get(), uri);
        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setCreateStreamIfNotExists(false);
        Namespace namespace = NamespaceBuilder.newBuilder()
            .conf(newConf).uri(uri).build();
        DistributedLogManager dlm = namespace.openLog(logName);
        LogWriter writer;
        try {
            writer = dlm.startLogSegmentNonPartitioned();
            writer.write(DLMTestUtil.getLogRecordInstance(1L));
            writer.commit();
            fail("Should fail to write data if stream doesn't exist.");
        } catch (IOException ioe) {
            // expected
        }
        dlm.close();
    }

    @Test(timeout = 60000)
    public void testCreateIfNotExists() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        ensureURICreated(zooKeeperClient.get(), uri);
        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setCreateStreamIfNotExists(false);
        String streamName = "test-stream";
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(newConf).uri(uri).build();
        DistributedLogManager dlm = namespace.openLog(streamName);
        LogWriter writer;
        try {
            writer = dlm.startLogSegmentNonPartitioned();
            writer.write(DLMTestUtil.getLogRecordInstance(1L));
            fail("Should fail to write data if stream doesn't exist.");
        } catch (IOException ioe) {
            // expected
        }
        dlm.close();

        // create the stream
        namespace.createLog(streamName);

        DistributedLogManager newDLM = namespace.openLog(streamName);
        LogWriter newWriter = newDLM.startLogSegmentNonPartitioned();
        newWriter.write(DLMTestUtil.getLogRecordInstance(1L));
        newWriter.close();
        newDLM.close();
    }

    @Test(timeout = 60000)
    public void testInvalidStreamName() throws Exception {
        assertFalse(DLUtils.isReservedStreamName("test"));
        assertTrue(DLUtils.isReservedStreamName(".test"));

        URI uri = createDLMURI("/" + runtime.getMethodName());

        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();

        try {
            namespace.openLog(".test1");
            fail("Should fail to create invalid stream .test");
        } catch (InvalidStreamNameException isne) {
            // expected
        }

        DistributedLogManager dlm = namespace.openLog("test1");
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        writer.write(DLMTestUtil.getLogRecordInstance(1));
        writer.close();
        dlm.close();

        try {
            namespace.openLog(".test2");
            fail("Should fail to create invalid stream .test2");
        } catch (InvalidStreamNameException isne) {
            // expected
        }

        try {
            namespace.openLog("/ test2");
            fail("Should fail to create invalid stream / test2");
        } catch (InvalidStreamNameException isne) {
            // expected
        }

        try {
            char[] chars = new char[6];
            for (int i = 0; i < chars.length; i++) {
                chars[i] = 'a';
            }
            chars[0] = 0;
            String streamName = new String(chars);
            namespace.openLog(streamName);
            fail("Should fail to create invalid stream " + streamName);
        } catch (InvalidStreamNameException isne) {
            // expected
        }

        try {
            char[] chars = new char[6];
            for (int i = 0; i < chars.length; i++) {
                chars[i] = 'a';
            }
            chars[3] = '\u0010';
            String streamName = new String(chars);
            namespace.openLog(streamName);
            fail("Should fail to create invalid stream " + streamName);
        } catch (InvalidStreamNameException isne) {
            // expected
        }

        DistributedLogManager newDLM =
                namespace.openLog("test_2-3");
        LogWriter newWriter = newDLM.startLogSegmentNonPartitioned();
        newWriter.write(DLMTestUtil.getLogRecordInstance(1));
        newWriter.close();
        newDLM.close();

        Iterator<String> streamIter = namespace.getLogs();
        Set<String> streamSet = Sets.newHashSet(streamIter);

        assertEquals(2, streamSet.size());
        assertTrue(streamSet.contains("test1"));
        assertTrue(streamSet.contains("test_2-3"));

        namespace.close();
    }

    @Test(timeout = 60000)
    public void testNamespaceListener() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        zooKeeperClient.get().create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(conf).uri(uri).build();
        final CountDownLatch[] latches = new CountDownLatch[3];
        for (int i = 0; i < 3; i++) {
            latches[i] = new CountDownLatch(1);
        }
        final AtomicInteger numUpdates = new AtomicInteger(0);
        final AtomicInteger numFailures = new AtomicInteger(0);
        final AtomicReference<Collection<String>> receivedStreams = new AtomicReference<Collection<String>>(null);
        namespace.registerNamespaceListener(new NamespaceListener() {
            @Override
            public void onStreamsChanged(Iterator<String> streams) {
                Set<String> streamSet = Sets.newHashSet(streams);
                int updates = numUpdates.incrementAndGet();
                if (streamSet.size() != updates - 1) {
                    numFailures.incrementAndGet();
                }

                receivedStreams.set(streamSet);
                latches[updates - 1].countDown();
            }
        });
        latches[0].await();
        namespace.createLog("test1");
        latches[1].await();
        namespace.createLog("test2");
        latches[2].await();
        assertEquals(0, numFailures.get());
        assertNotNull(receivedStreams.get());
        Set<String> streamSet = new HashSet<String>();
        streamSet.addAll(receivedStreams.get());
        assertEquals(2, receivedStreams.get().size());
        assertEquals(2, streamSet.size());
        assertTrue(streamSet.contains("test1"));
        assertTrue(streamSet.contains("test2"));
    }

    private void initDlogMeta(String dlNamespace, String un, String streamName) throws Exception {
        URI uri = createDLMURI(dlNamespace);
        DistributedLogConfiguration newConf = new DistributedLogConfiguration();
        newConf.addConfiguration(conf);
        newConf.setCreateStreamIfNotExists(true);
        newConf.setZkAclId(un);
        Namespace namespace = NamespaceBuilder.newBuilder()
                .conf(newConf).uri(uri).build();
        DistributedLogManager dlm = namespace.openLog(streamName);
        LogWriter writer = dlm.startLogSegmentNonPartitioned();
        for (int i = 0; i < 10; i++) {
            writer.write(DLMTestUtil.getLogRecordInstance(1L));
        }
        writer.close();
        dlm.close();
        namespace.close();
    }

    @Test(timeout = 60000)
    public void testAclPermsZkAccessConflict() throws Exception {

        String namespace = "/" + runtime.getMethodName();
        initDlogMeta(namespace, "test-un", "test-stream");
        URI uri = createDLMURI(namespace);

        ZooKeeperClient zkc = TestZooKeeperClientBuilder.newBuilder()
            .name("unpriv")
            .uri(uri)
            .build();

        try {
            zkc.get().create(uri.getPath() + "/test-stream/test-garbage",
                new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            fail("write should have failed due to perms");
        } catch (KeeperException.NoAuthException ex) {
            LOG.info("caught exception trying to write with no perms", ex);
        }

        try {
            zkc.get().setData(uri.getPath() + "/test-stream", new byte[0], 0);
            fail("write should have failed due to perms");
        } catch (KeeperException.NoAuthException ex) {
            LOG.info("caught exception trying to write with no perms", ex);
        }
    }

    @Test(timeout = 60000)
    public void testAclPermsZkAccessNoConflict() throws Exception {

        String namespace = "/" + runtime.getMethodName();
        initDlogMeta(namespace, "test-un", "test-stream");
        URI uri = createDLMURI(namespace);

        ZooKeeperClient zkc = TestZooKeeperClientBuilder.newBuilder()
            .name("unpriv")
            .uri(uri)
            .build();

        zkc.get().getChildren(uri.getPath() + "/test-stream", false, new Stat());
        zkc.get().getData(uri.getPath() + "/test-stream", false, new Stat());
    }

    @Test(timeout = 60000)
    public void testAclModifyPermsDlmConflict() throws Exception {
        String streamName = "test-stream";

        // Reopening and writing again with the same un will succeed.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);

        try {
            // Reopening and writing again with a different un will fail.
            initDlogMeta("/" + runtime.getMethodName(), "not-test-un", streamName);
            fail("Write should have failed due to perms");
        } catch (ZKException ex) {
            LOG.info("Caught exception trying to write with no perms", ex);
            assertEquals(KeeperException.Code.NOAUTH, ex.getKeeperExceptionCode());
        } catch (Exception ex) {
            LOG.info("Caught wrong exception trying to write with no perms", ex);
            fail("Wrong exception " + ex.getClass().getName() + " expected " + LockingException.class.getName());
        }

        // Should work again.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);
    }

    @Test(timeout = 60000)
    public void testAclModifyPermsDlmNoConflict() throws Exception {
        String streamName = "test-stream";

        // Establish the uri.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);

        // Reopening and writing again with the same un will succeed.
        initDlogMeta("/" + runtime.getMethodName(), "test-un", streamName);
    }

    static void validateBadAllocatorConfiguration(DistributedLogConfiguration conf, URI uri) throws Exception {
        try {
            BKNamespaceDriver.validateAndGetFullLedgerAllocatorPoolPath(conf, uri);
            fail("Should throw exception when bad allocator configuration provided");
        } catch (IOException ioe) {
            // expected
        }
    }

    @Test(timeout = 60000)
    public void testValidateAndGetFullLedgerAllocatorPoolPath() throws Exception {
        DistributedLogConfiguration testConf = new DistributedLogConfiguration();
        testConf.setEnableLedgerAllocatorPool(true);

        String namespace = "/" + runtime.getMethodName();
        URI uri = createDLMURI(namespace);

        testConf.setLedgerAllocatorPoolName("test");

        testConf.setLedgerAllocatorPoolPath("test");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath(".");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath("..");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath("./");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath(".test/");
        validateBadAllocatorConfiguration(testConf, uri);

        testConf.setLedgerAllocatorPoolPath(".test");
        testConf.setLedgerAllocatorPoolName(null);
        validateBadAllocatorConfiguration(testConf, uri);
    }

    @Test(timeout = 60000)
    public void testUseNamespaceAfterCloseShouldFailFast() throws Exception {
        URI uri = createDLMURI("/" + runtime.getMethodName());
        Namespace namespace = NamespaceBuilder.newBuilder()
            .conf(conf)
            .uri(uri)
            .build();
        // before closing the namespace, no exception should be thrown
        String logName = "test-stream";
        // create a log
        namespace.createLog(logName);
        // log exists
        Assert.assertTrue(namespace.logExists(logName));
        // create a dlm
        DistributedLogManager dlm = namespace.openLog(logName);
        // do some writes
        BKAsyncLogWriter writer = (BKAsyncLogWriter) (dlm.startAsyncLogSegmentNonPartitioned());
        for (long i = 0; i < 3; i++) {
            LogRecord record = DLMTestUtil.getLargeLogRecordInstance(i);
            writer.write(record);
        }
        writer.closeAndComplete();
        // do some reads
        LogReader reader = dlm.getInputStream(0);
        for (long i = 0; i < 3; i++) {
            Assert.assertEquals(reader.readNext(false).getTransactionId(), i);
        }
        namespace.deleteLog(logName);
        Assert.assertFalse(namespace.logExists(logName));

        // now try to close the namespace
        namespace.close();
        try {
            namespace.createLog(logName);
            fail("Should throw exception after namespace is closed");
        } catch (AlreadyClosedException e) {
            // No-ops
        }
        try {
            namespace.openLog(logName);
            fail("Should throw exception after namespace is closed");
        } catch (AlreadyClosedException e) {
            // No-ops
        }
        try {
            namespace.logExists(logName);
            fail("Should throw exception after namespace is closed");
        } catch (AlreadyClosedException e) {
            // No-ops
        }
        try {
            namespace.getLogs();
            fail("Should throw exception after namespace is closed");
        } catch (AlreadyClosedException e) {
            // No-ops
        }
        try {
            namespace.deleteLog(logName);
            fail("Should throw exception after namespace is closed");
        } catch (AlreadyClosedException e) {
            // No-ops
        }
        try {
            namespace.createAccessControlManager();
            fail("Should throw exception after namespace is closed");
        } catch (AlreadyClosedException e) {
            // No-ops
        }
    }
}
