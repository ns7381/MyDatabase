
package shardingsphere.workshop.mysql.proxy;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import lombok.RequiredArgsConstructor;
import shardingsphere.workshop.mysql.proxy.handler.FrontendChannelInboundHandler;
import shardingsphere.workshop.mysql.proxy.handler.MySQLPacketCodec;

/**
 * Channel initializer.
 */
@RequiredArgsConstructor
public final class ServerHandlerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(final SocketChannel socketChannel) {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new MySQLPacketCodec());
        pipeline.addLast(new FrontendChannelInboundHandler());
    }
}
