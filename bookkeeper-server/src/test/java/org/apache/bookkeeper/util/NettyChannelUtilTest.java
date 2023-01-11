package org.apache.bookkeeper.util;

import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.channel.ChannelPromise;
import io.netty.channel.VoidChannelPromise;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class NettyChannelUtilTest {

    @Test
    public void testWriteAndFlushWithVoidPromise() {
        final ChannelOutboundInvoker ctx = mock(ChannelOutboundInvoker.class);
        final VoidChannelPromise voidChannelPromise = new VoidChannelPromise(mock(Channel.class), true);
        when(ctx.voidPromise()).thenReturn(voidChannelPromise);
        final byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        try {
            NettyChannelUtil.writeAndFlushWithVoidPromise(ctx, byteBuf);
            verify(ctx).writeAndFlush(same(byteBuf), same(voidChannelPromise));
            verify(ctx).voidPromise();
        } finally {
            byteBuf.release();
        }
    }

    @Test
    public void testWriteAndFlushWithClosePromise() {
        final ChannelOutboundInvoker ctx = mock(ChannelOutboundInvoker.class);
        final ChannelPromise promise = mock(ChannelPromise.class);

        final byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        final ByteBuf byteBuf = Unpooled.wrappedBuffer(data, 0, data.length);
        when(ctx.writeAndFlush(same(byteBuf))).thenReturn(promise);
        try {
            NettyChannelUtil.writeAndFlushWithClosePromise(ctx, byteBuf);
            verify(ctx).writeAndFlush(same(byteBuf));
            verify(promise).addListener(same(ChannelFutureListener.CLOSE));
        } finally {
            byteBuf.release();
        }
    }

}