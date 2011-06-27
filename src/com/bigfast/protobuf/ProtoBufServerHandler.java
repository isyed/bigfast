package com.bigfast.protobuf;

import com.bigfast.cache.OffHeapCache;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import com.bigfast.protobuf.OffHeapCacheProtoBuf.Operation;
import com.bigfast.protobuf.OffHeapCacheProtoBuf.Operations;
import com.bigfast.protobuf.OffHeapCacheProtoBuf.Result;
import com.bigfast.protobuf.OffHeapCacheProtoBuf.Results;

/**
 * 
 * @author isyed
 * @version 0.1
 */
public class ProtoBufServerHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = Logger.getLogger(
            ProtoBufServerHandler.class.getName());

    @Override
    public void handleUpstream(
            ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
        if (e instanceof ChannelStateEvent) {
            logger.info(e.toString());
        }
        super.handleUpstream(ctx, e);
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx, MessageEvent e) {
        Operations operations = (Operations) e.getMessage();
        Results.Builder builder = Results.newBuilder();
        for (Operation o : operations.getOperationList()) {
            if (o.getGetKey().equals("")) {
                builder.addResult(Result.newBuilder().
                        setPutResult(OffHeapCache.getOffHeapCache().put(o.getPutKey(), o.getPutValue())).
                        build());
            } else if (o.getPutKey().equals("")) {
                builder.addResult(Result.newBuilder().
                        setGetResult(OffHeapCache.getOffHeapCache().get(o.getGetKey())).build());
            }
        }
        e.getChannel().write(builder.build());
    }

    @Override
    public void exceptionCaught(
            ChannelHandlerContext ctx, ExceptionEvent e) {
        logger.log(
                Level.WARNING,
                "Unexpected exception from downstream.",
                e.getCause());
        e.getChannel().close();
    }
}
