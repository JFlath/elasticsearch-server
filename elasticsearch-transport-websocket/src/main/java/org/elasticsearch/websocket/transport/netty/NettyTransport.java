package org.elasticsearch.websocket.transport.netty;

import java.net.SocketAddress;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportServiceAdapter;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;

public interface NettyTransport extends Transport {
    
    Settings settings();

    ThreadPool threadPool();
    
    TransportServiceAdapter transportServiceAdapter();
    
    boolean compress();
    
    TransportAddress wrapAddress(SocketAddress socketAddress);
    
    void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception;
}
