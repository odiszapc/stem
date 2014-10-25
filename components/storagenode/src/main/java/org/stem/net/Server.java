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

package org.stem.net;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.transport.*;

import java.net.InetSocketAddress;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    InetSocketAddress socket;
    PacketDecoder packetDecoder = new PacketDecoder();
    PacketEncoder packetEncoder = new PacketEncoder();
    MessageDecoder messageDecoder = new MessageDecoder();
    MessageEncoder messageEncoder = new MessageEncoder();
    MessageDispatcher dispatcher = new MessageDispatcher();
    private ChannelFuture future;
    private Channel channel;

    public Server(String host, int port) {
        socket = new InetSocketAddress(host, port);
    }

    public void start() {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 100)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new PacketDecoder())
                                .addLast(new PacketEncoder())

                                .addLast(new MessageDecoder())
                                .addLast(new MessageEncoder())

                                .addLast(new MessageDispatcher());
                    }
                });

        try {
            future = bootstrap.bind(socket).sync();
            logger.info("Starting listening for clients on {}...", socket);
            channel = future.channel();

            // Wait until server socket is closed.
            // channel.closeFuture().sync();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException("Can't start server: ", e);
        }

    }

    public void stop() {
        channel.close();
        //future.awaitUninterruptibly();
    }
}
