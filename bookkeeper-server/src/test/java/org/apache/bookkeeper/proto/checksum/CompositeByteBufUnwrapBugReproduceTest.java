/*
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
 */

package org.apache.bookkeeper.proto.checksum;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.scurrilous.circe.checksum.IntHash;
import com.scurrilous.circe.checksum.Java8IntHash;
import com.scurrilous.circe.checksum.Java9IntHash;
import com.scurrilous.circe.checksum.JniIntHash;
import com.scurrilous.circe.crc.Sse42Crc32C;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCounted;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.bookkeeper.proto.BookieProtoEncoding;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.ByteBufVisitor;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This test class was added to reproduce a bug in the checksum calculation when
 * the payload is a CompositeByteBuf and this buffer has a reader index state other than 0.
 * The reader index state gets lost in the unwrapping process.
 *
 * There were at least 2 different bugs. One that occurred when the
 * payload was >= BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD and the other when
 * it was < BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD.
 * This test covers both useV2Protocol=true and useV2Protocol=false since the bug was triggered differently.
 *
 * The bug has been fixed and this test is here to make sure it doesn't happen again.
 */
@RunWith(Parameterized.class)
public class CompositeByteBufUnwrapBugReproduceTest {
    final byte[] testPayLoad;
    final int defaultBufferPrefixLength;
    private final boolean useV2Protocol;

    // set to 0 to 3 to run a single scenario for debugging purposes
    private static final int RUN_SINGLE_SCENARIO_FOR_DEBUGGING = -1;

    @Parameterized.Parameters
    public static Collection<Object[]> testScenarios() {
        List<Object[]> scenarios = Arrays.asList(new Object[][] {
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD - 1, true},
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD - 1, false},
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD, true},
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD, false}
        });
        if (RUN_SINGLE_SCENARIO_FOR_DEBUGGING >= 0) {
            // pick a single scenario for debugging
            scenarios = scenarios.subList(RUN_SINGLE_SCENARIO_FOR_DEBUGGING, 1);
        }
        return scenarios;
    }

    public CompositeByteBufUnwrapBugReproduceTest(int payloadSize, boolean useV2Protocol) {
        this.testPayLoad = createTestPayLoad(payloadSize);
        this.defaultBufferPrefixLength = payloadSize / 7;
        this.useV2Protocol = useV2Protocol;
    }

    private static byte[] createTestPayLoad(int payloadSize) {
        byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payloadSize; i++) {
            payload[i] = (byte) i;
        }
        return payload;
    }


    /**
     * A DigestManager that uses the given IntHash implementation for testing.
     */
    static class TestIntHashDigestManager extends DigestManager {
        private final IntHash intHash;

        public TestIntHashDigestManager(IntHash intHash, long ledgerId, boolean useV2Protocol,
                                        ByteBufAllocator allocator) {
            super(ledgerId, useV2Protocol, allocator);
            this.intHash = intHash;
        }

        @Override
        int getMacCodeLength() {
            return 4;
        }

        @Override
        boolean isInt32Digest() {
            return true;
        }

        @Override
        void populateValueAndReset(int digest, ByteBuf buf) {
            buf.writeInt(digest);
        }

        @Override
        int internalUpdate(int digest, ByteBuf data, int offset, int len) {
            return intHash.resume(digest, data, offset, len);
        }

        @Override
        int internalUpdate(int digest, byte[] buffer, int offset, int len) {
            return intHash.resume(digest, buffer, offset, len);
        }

        @Override
        boolean acceptsMemoryAddressBuffer() {
            return intHash.acceptsMemoryAddressBuffer();
        }
    }

    @Test
    public void shouldCalculateChecksumForCompositeBuffer() {
        ByteBuf testPayload = Unpooled.wrappedBuffer(testPayLoad);
        byte[] referenceOutput = computeDigestAndPackageForSending(new Java8IntHash(), testPayload.retainedDuplicate());
        assertDigestAndPackageMatchesReference(new Java8IntHash(), testPayload, referenceOutput);
        assertDigestAndPackageMatchesReference(new Java9IntHash(), testPayload, referenceOutput);
        if (Sse42Crc32C.isSupported()) {
            assertDigestAndPackageMatchesReference(new JniIntHash(), testPayload, referenceOutput);
        }
        testPayload.release();
    }

    private void assertDigestAndPackageMatchesReference(IntHash intHash, ByteBuf payload, byte[] referenceOutput) {
        assertDigestAndPackageScenario(intHash, payload.retainedDuplicate(), referenceOutput, testPayLoad,
                "plain payload, no wrapping");

        ByteBuf payload2 = wrapWithPrefixAndCompositeByteBufWithReaderIndexState(payload.retainedDuplicate(),
                defaultBufferPrefixLength);
        assertDigestAndPackageScenario(intHash, payload2, referenceOutput, testPayLoad,
                "payload with prefix wrapped in CompositeByteBuf with readerIndex state");

        ByteBuf payload3 = wrapWithPrefixAndMultipleCompositeByteBufWithReaderIndexStateAndMultipleLayersOfDuplicate(
                payload.retainedDuplicate(), defaultBufferPrefixLength);
        assertDigestAndPackageScenario(intHash, payload3, referenceOutput, testPayLoad,
                "payload with prefix wrapped in 2 layers of CompositeByteBuf with readerIndex state in the outer "
                        + "composite. In addition, the outer composite is duplicated twice.");

        ByteBuf payload4 = wrapInCompositeByteBufAndSlice(payload.retainedDuplicate(), defaultBufferPrefixLength);
        assertDigestAndPackageScenario(intHash, payload4, referenceOutput, testPayLoad,
                "payload with prefix wrapped in CompositeByteBuf and sliced");
    }

    private void assertDigestAndPackageScenario(IntHash intHash, ByteBuf payload, byte[] referenceOutput,
                                                byte[] testPayLoadArray,
                                                String scenario) {
        // this validates that the readable bytes in the payload match the TEST_PAYLOAD content
        assertArrayEquals(testPayLoadArray, ByteBufUtil.getBytes(payload.duplicate()),
                "input is invalid for scenario '" + scenario + "'");

        ByteBuf visitedCopy = Unpooled.buffer(payload.readableBytes());
        ByteBufVisitor.visitBuffers(payload, payload.readerIndex(), payload.readableBytes(),
                new ByteBufVisitor.ByteBufVisitorCallback<Void>() {
                    @Override
                    public void visitBuffer(Void context, ByteBuf visitBuffer, int visitIndex, int visitLength) {
                        visitedCopy.writeBytes(visitBuffer, visitIndex, visitLength);
                    }

                    @Override
                    public void visitArray(Void context, byte[] visitArray, int visitIndex, int visitLength) {
                        visitedCopy.writeBytes(visitArray, visitIndex, visitLength);
                    }
                }, null);

        assertArrayEquals(ByteBufUtil.getBytes(visitedCopy), testPayLoadArray,
                "visited copy is invalid for scenario '" + scenario + "'. Bug in ByteBufVisitor?");

        // compute the digest and package
        byte[] output = computeDigestAndPackageForSending(intHash, payload.duplicate());
        if (referenceOutput == null) {
            referenceOutput =
                    computeDigestAndPackageForSending(new Java8IntHash(), Unpooled.wrappedBuffer(testPayLoadArray));
        }
        // this validates that the output matches the reference output
        assertArrayEquals(referenceOutput, output, "output is invalid for scenario '" + scenario + "'");
    }

    private byte[] computeDigestAndPackageForSending(IntHash intHash, ByteBuf data) {
        DigestManager digestManager = new TestIntHashDigestManager(intHash, 1, useV2Protocol, ByteBufAllocator.DEFAULT);
        ReferenceCounted packagedBuffer =
                digestManager.computeDigestAndPackageForSending(1, 0, data.readableBytes(), data,
                        MacDigestManager.EMPTY_LEDGER_KEY, BookieProtocol.FLAG_NONE);
        return packagedBufferToBytes(packagedBuffer);
    }

    ByteBuf wrapWithPrefixAndCompositeByteBufWithReaderIndexState(ByteBuf payload, int bufferPrefixLength) {
        // create a new buffer with a prefix and the actual payload
        ByteBuf prefixedPayload = ByteBufAllocator.DEFAULT.buffer(bufferPrefixLength + payload.readableBytes());
        prefixedPayload.writeBytes(RandomUtils.nextBytes(bufferPrefixLength));
        prefixedPayload.writeBytes(payload);

        // wrap the buffer in a composite buffer
        CompositeByteBuf outerComposite = ByteBufAllocator.DEFAULT.compositeBuffer();
        outerComposite.addComponent(true, prefixedPayload);

        // set reader index state. this is the state that gets lost in the unwrapping process
        outerComposite.readerIndex(bufferPrefixLength);

        return outerComposite;
    }

    ByteBuf wrapWithPrefixAndMultipleCompositeByteBufWithReaderIndexStateAndMultipleLayersOfDuplicate(ByteBuf payload,
                                                                                               int bufferPrefixLength) {
        // create a new buffer with a prefix and the actual payload
        ByteBuf prefixedPayload = ByteBufAllocator.DEFAULT.buffer(bufferPrefixLength + payload.readableBytes());
        prefixedPayload.writeBytes(RandomUtils.nextBytes(bufferPrefixLength));
        prefixedPayload.writeBytes(payload);

        CompositeByteBuf innerComposite = ByteBufAllocator.DEFAULT.compositeBuffer();
        innerComposite.addComponent(true, prefixedPayload);
        innerComposite.addComponent(true, Unpooled.EMPTY_BUFFER);

        // wrap the buffer in a composite buffer
        CompositeByteBuf outerComposite = ByteBufAllocator.DEFAULT.compositeBuffer();
        outerComposite.addComponent(true, innerComposite);
        outerComposite.addComponent(true, Unpooled.EMPTY_BUFFER);

        // set reader index state. this is the state that gets lost in the unwrapping process
        outerComposite.readerIndex(bufferPrefixLength);

        return outerComposite.duplicate().duplicate();
    }

    ByteBuf wrapInCompositeByteBufAndSlice(ByteBuf payload, int bufferPrefixLength) {
        // create a composite buffer
        CompositeByteBuf compositeWithPrefix = ByteBufAllocator.DEFAULT.compositeBuffer();
        compositeWithPrefix.addComponent(true, Unpooled.wrappedBuffer(RandomUtils.nextBytes(bufferPrefixLength)));
        compositeWithPrefix.addComponent(true, payload);

        // return a slice of the composite buffer so that it returns the payload
        return compositeWithPrefix.slice(bufferPrefixLength, payload.readableBytes());
    }

    private static byte[] packagedBufferToBytes(ReferenceCounted packagedBuffer) {
        byte[] output;
        if (packagedBuffer instanceof ByteBufList) {
            ByteBufList bufList = (ByteBufList) packagedBuffer;
            output = new byte[bufList.readableBytes()];
            bufList.getBytes(output);
            for (int i = 0; i < bufList.size(); i++) {
                bufList.getBuffer(i).release();
            }
        } else if (packagedBuffer instanceof ByteBuf) {
            output = ByteBufUtil.getBytes((ByteBuf) packagedBuffer);
            packagedBuffer.release();
        } else {
            throw new RuntimeException("Unexpected type: " + packagedBuffer.getClass());
        }
        return output;
    }
}