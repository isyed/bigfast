package com.bigfast.protobuf;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * Provides ProtoBuf based access to the OffHeapCache. The default port which the server runs on is 9091.
 *
 * @author isyed
 * @version 0.1
 */
public final class ProtoBufServer {

    private static String host = "localhost";
    private static int port = 9091;
    public static final ProtoBufServer SERVER = new ProtoBufServer();

    private ProtoBufServer() {
        // Configure the server.
        ServerBootstrap bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));

        // Set up the event pipeline factory.
        bootstrap.setPipelineFactory(new ProtoBufServerPipelineFactory());

        // Bind and start to accept incoming connections.
        bootstrap.bind(new InetSocketAddress(host, port));
    }

    public static ProtoBufServer getInstance() {
        return ProtoBufServer.SERVER;
    }

    public static void main(String[] args) {
        getInstance();
    }
}
