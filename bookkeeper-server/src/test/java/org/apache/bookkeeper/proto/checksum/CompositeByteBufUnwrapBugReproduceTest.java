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
import org.apache.bookkeeper.proto.BookieProtoEncoding;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Reproduces a bug in the checksum calculation when the payload is
 * a CompositeByteBuf and this buffer has a reader index state other than 0.
 * The reader index state gets lost in the unwrapping process.
 *
 * There are at least 2 different bugs. One that occurs when the
 * payload is >= BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD and the other when
 * it's < BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD.
 * This test covers both useV2Protocol=true and useV2Protocol=false since the bug is triggered differently.
 */
@RunWith(Parameterized.class)
public class CompositeByteBufUnwrapBugReproduceTest {
    final byte[] testPayLoad;
    final int bufferPrefixLength;
    private final boolean useV2Protocol;

    @Parameterized.Parameters
    public static Collection<Object[]> testScenarios() {
        return Arrays.asList(new Object[][] {
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD - 1, true},
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD - 1, false},
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD, true},
                {BookieProtoEncoding.SMALL_ENTRY_SIZE_THRESHOLD, false}
        });
    }

    public CompositeByteBufUnwrapBugReproduceTest(int payloadSize, boolean useV2Protocol) {
        this.testPayLoad = createTestPayLoad(payloadSize);
        this.bufferPrefixLength = payloadSize / 7;
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

        public TestIntHashDigestManager(IntHash intHash, long ledgerId, boolean useV2Protocol, ByteBufAllocator allocator) {
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
        int update(int digest, ByteBuf data, int offset, int len) {
            return intHash.resume(digest, data, offset, len);
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
        byte[] output1 = computeDigestAndPackageForSending(intHash, payload.retainedDuplicate());
        assertArrayEquals(referenceOutput, output1);

        // wrap the payload in a CompositeByteBuf which contains another buffer where the payload contains a prefix
        // the reader index is set past the prefix
        ByteBuf payload2 = wrapWithPrefixAndCompositeByteBufWithReaderIndexState(payload.retainedDuplicate());
        // this validates that the readable bytes in payload2 match the TEST_PAYLOAD content
        assertArrayEquals(ByteBufUtil.getBytes(payload2.duplicate()), testPayLoad);

        byte[] output2 = computeDigestAndPackageForSending(intHash, payload2);
        assertArrayEquals(output1, output2);
    }

    private byte[] computeDigestAndPackageForSending(IntHash intHash, ByteBuf data) {
        DigestManager digestManager = new TestIntHashDigestManager(intHash, 1, useV2Protocol, ByteBufAllocator.DEFAULT);
        ReferenceCounted packagedBuffer =
                digestManager.computeDigestAndPackageForSending(1, 0, data.readableBytes(), data,
                        MacDigestManager.EMPTY_LEDGER_KEY, BookieProtocol.FLAG_NONE);
        return packagedBufferToBytes(packagedBuffer);
    }

    ByteBuf wrapWithPrefixAndCompositeByteBufWithReaderIndexState(ByteBuf payload) {
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