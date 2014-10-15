/*
 * Copyright 2014 Alexey Plotnik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.stem.client.v2;

import com.google.common.util.concurrent.AbstractFuture;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.exceptions.ClientTransportException;
import org.stem.exceptions.ConnectionException;
import org.stem.transport.*;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public class Connection
{
    private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    private InetSocketAddress address;
    private Factory factory;
    protected Channel channel;
    private final Dispatcher dispatcher = new Dispatcher();
    private volatile boolean isReady;
    private volatile boolean isDisconnecting;
    private final AtomicReference<ConnectionCloseFuture> closeFutureRef = new AtomicReference<>();

    public Connection(InetSocketAddress address, Factory factory) throws ConnectionException
    {
        this.address = address;
        this.factory = factory;

        Bootstrap bootstrap = factory.createNewBootstrap();
        bootstrap
                .group(new NioEventLoopGroup())
                .channel(NioSocketChannel.class)
                .handler(new ChannelHandler(this));

        ChannelFuture future = bootstrap.connect(address);
        this.channel = future.awaitUninterruptibly().channel();
        if (!future.isSuccess())
        {
            throw disconnecting(new ClientTransportException(address, "Can't connect", future.cause()));
        }
        isReady = true;
    }

    private <E extends Exception> E disconnecting(E e)
    {
        isDisconnecting = true;

        close().force();

        return e;
    }

    public CloseFuture close()
    {
        ConnectionCloseFuture future = new ConnectionCloseFuture();
        if (!closeFutureRef.compareAndSet(null, future))
        {
            return closeFutureRef.get();
        }

        logger.debug("Closing connection");

        //if (dispatcher.pending.isEmpty())
        //    future.force();
        return future;
    }

    private class ConnectionCloseFuture extends CloseFuture
    {

        @Override
        public ConnectionCloseFuture force()
        {
            return null;
        }
    }

    public static class ChannelHandler extends ChannelInitializer<SocketChannel>
    {

        private static final PacketDecoder packetDecoder = new PacketDecoder();
        private static final PacketEncoder packetEncoder = new PacketEncoder();
        private static final MessageDecoder messageDecoder = new MessageDecoder();
        private static final MessageEncoder messageEncoder = new MessageEncoder();
        private Connection connection;

        public ChannelHandler(Connection connection)
        {
            this.connection = connection;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception
        {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("packetDecoder", packetDecoder);
            pipeline.addLast("packetEncoder", packetEncoder);
            pipeline.addLast("messageDecoder", messageDecoder);
            pipeline.addLast("messageEncoder", messageEncoder);
            pipeline.addLast("dispatcher", connection.dispatcher);

        }
    }

    public static class Factory
    {
        public Bootstrap createNewBootstrap()
        {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_LINGER, 0)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.SO_RCVBUF, null)
                    .option(ChannelOption.SO_SNDBUF, null);
            return bootstrap;
        }
    }

    private class Dispatcher extends SimpleChannelInboundHandler<Message.Response>
    {
        private final ConcurrentMap<Integer, ResponseHandler> pending = new ConcurrentHashMap<Integer, ResponseHandler>();
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Response message) throws Exception
        {

        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception
        {
            logger.info("Unexpected exception caught {}", e);
            disconnecting(new ClientTransportException(address, String.format("Unexpected exception %s", e.getCause()), e.getCause()));
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
        {
            logger.info("Channel unregistered");
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception
        {
            logger.info("Channel closed");
        }
    }

    static class Future extends AbstractFuture<Message.Response> implements RequestHandler.Callback {

        @Override
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency)
        {

        }

        @Override
        public void register(RequestHandler handler)
        {

        }

        @Override
        public Message.Request request()
        {
            return null;
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency)
        {

        }

        @Override
        public void onException(Connection connection, Exception exception, long latency)
        {

        }

        @Override
        public void onTimeout(Connection connection, long latency)
        {

        }
    }

    interface ResponseCallback {
        public Message.Request request();
        public void onSet(Connection connection, Message.Response response, long latency);
        public void onException(Connection connection, Exception exception, long latency);
        public void onTimeout(Connection connection, long latency);
    }

    static class ResponseHandler
    {
    }
}
