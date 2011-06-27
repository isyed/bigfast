package com.bigfast.protobuf;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

/**
 * A Sample Client showing how to access (put/get operations) the OffHeapCache via ProtoBufs
 * @author isyed
 * @version 0.1
 */
public class ProtoBufClient {

    final static String host = "localhost";
    final static int port = 9091;

    public static void main(String[] args) throws Exception {
        Collection<String> operations = new ArrayList<String>();

        //SAMPLE OPERATIONS
        operations.add("put/FirstKey/FirstValue");
        operations.add("get/FirstKey");

        // Set up.
        ClientBootstrap bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));

        // Configure the event pipeline factory.
        bootstrap.setPipelineFactory(new ProtoBufClientPipelineFactory());

        // Make a new connection.
        ChannelFuture connectFuture =
                bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection is made successfully.
        Channel channel = connectFuture.awaitUninterruptibly().getChannel();

        // Get the handler instance to initiate the request.
        ProtoBufClientHandler handler =
                channel.getPipeline().get(ProtoBufClientHandler.class);

        // Request and get the response.
        List<String> response = handler.accessCache(operations);

        // Close the connection.
        channel.close().awaitUninterruptibly();

        // Shut down all thread pools to exit.
        bootstrap.releaseExternalResources();

        // Print the response.
        Iterator<String> i1 = operations.iterator();
        Iterator<String> i2 = response.iterator();
        while (i1.hasNext()) {
            System.out.format("%28s: %s%n", i1.next(), i2.next());
        }
    }
}
