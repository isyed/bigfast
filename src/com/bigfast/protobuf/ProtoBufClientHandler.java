package com.bigfast.protobuf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jboss.netty.channel.Channel;
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
public class ProtoBufClientHandler extends SimpleChannelUpstreamHandler {

    private static final Logger logger = Logger.getLogger(
            ProtoBufClientHandler.class.getName());

    private volatile Channel channel;
    private final BlockingQueue<Results> answer = new LinkedBlockingQueue<Results>();

    public List<String> accessCache(Collection<String> operations) {
        Operations.Builder builder = Operations.newBuilder();
        for (String c : operations) {
            String[] components = c.split("/");
            if (components[0].equals("put")) {
                builder.addOperation(Operation.newBuilder().
                        setPutKey(components[1]).
                        setPutValue(components[2]).build());

            } else if (components[0].equals("get")) {
                builder.addOperation(Operation.newBuilder().
                        setGetKey(components[1]).build());
            }

        }
        channel.write(builder.build());

        Results results;
        boolean interrupted = false;
        for (;;) {
            try {
                results = answer.take();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        List<String> result = new ArrayList<String>();
        for (Result opResult : results.getResultList()) {
            if (opResult.getGetResult().equals("")) {
                result.add(opResult.getPutResult());
            } else if (opResult.getPutResult().equals("")) {
                result.add(opResult.getGetResult());
            }
        }
        System.out.println();
        return result;
    }

    @Override
    public void handleUpstream(
            ChannelHandlerContext ctx, ChannelEvent e)
            throws Exception {
        if (e instanceof ChannelStateEvent) {
            logger.info(e.toString());
        }
        super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception {
        channel = e.getChannel();
        super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(
            ChannelHandlerContext ctx,
            final MessageEvent e) {
        boolean offered = answer.offer((Results) e.getMessage());
        assert offered;
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
