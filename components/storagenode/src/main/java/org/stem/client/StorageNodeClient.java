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

package org.stem.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.domain.BlobDescriptor;
import org.stem.exceptions.TimeoutException;
import org.stem.exceptions.TransportException;
import org.stem.transport.*;
import org.stem.transport.ops.*;
import org.stem.utils.Utils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class StorageNodeClient {

    private static final Logger logger = LoggerFactory.getLogger(StorageNodeClient.class);

    public final String host;
    public final int port;

    AtomicInteger requestCounter = new AtomicInteger(0);

    PacketDecoder packetDecoder = new PacketDecoder();
    PacketEncoder packetEncoder = new PacketEncoder();
    MessageDecoder messageDecoder = new MessageDecoder();
    MessageEncoder messageEncoder = new MessageEncoder();
    Dispatcher dispatcher = new Dispatcher();

    protected Channel channel;
    private volatile boolean connected;
    private Bootstrap bootstrap;

    public StorageNodeClient(String host, int port) {
        this.host = host;
        this.port = port;
        initBootstrap();
    }

    public StorageNodeClient(String endpoint) {
        this.host = Utils.getHost(endpoint);
        this.port = Utils.getPort(endpoint);
        initBootstrap();
    }

    private void initBootstrap() {
        bootstrap = new Bootstrap();

        EventLoopGroup workerGroup = new NioEventLoopGroup();

        bootstrap
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                        //.option(ChannelOption.SO_TIMEOUT, 100)

                .handler(new ChannelFactory());
    }

    public void start() throws IOException {
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));
        channel = future.awaitUninterruptibly().channel();


        if (!future.isSuccess()) {
            throw new IOException("Connection failed", future.cause());
        }
        this.connected = true;
    }

    public void stop() {
        channel.disconnect();
    }

    public Message.Response executeSimple(Message.Request operation) {
        try {
            long start = System.nanoTime();
            ChannelFuture future = channel.writeAndFlush(operation);
            Message.Response response = dispatcher.responses.take();
            if (response instanceof ErrorMessage)
                throw new RuntimeException((Throwable) ((ErrorMessage) response).error);

            float duration = (float) (System.nanoTime() - start) / 1000000;
            //logger.debug("Request took {} ms", String.format("%.3f", duration));
            return response;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //channel.flush();
    }

    // TODO: what should we return from a remote node if internal server error occurred on a remote side
    public BlobDescriptor writeBlob(WriteBlobMessage request) {
        ResultMessage.WriteBlob result = (ResultMessage.WriteBlob) execute(request);
        return new BlobDescriptor(result.getFatFileIndex(), -1, result.getOffset()); // TODO: remove -1 part
    }


    public void deleteBlob(DeleteBlobMessage request) {
        execute(request);
    }

    public byte[] readBlob(UUID diskId, int fatFileIndex, int offset, int length) {
        ReadBlobMessage message = new ReadBlobMessage(diskId, fatFileIndex, offset, length);
        ResultMessage.ReadBlob result = (ResultMessage.ReadBlob) execute(message);
        return result.getData();
    }

    public Message.Response execute(Message.Request request) {
        return execute(request, false);
    }


    public Message.Response execute(Message.Request request, boolean suppressExceptions) {
        try {
            if (!connected)
                start();

            request.setStreamId(getNextStreamId());
            RequestFuture requestFuture = new RequestFuture(request, 10000);
            dispatcher.add(requestFuture.callback);
            channel.writeAndFlush(request); // TODO: .addListener(futureListener());

            Message.Response response = requestFuture.getUninterruptibly();

            if (response instanceof ErrorMessage) {
                ErrorMessage errorResp = (ErrorMessage) response;

                if (suppressExceptions)
                    return errorResp;
                else {
                    TransportException error = errorResp.getError();
                    throw new RuntimeException(error.getMessage());
                }
            }

            return response;
        } catch (Exception e) {
            RuntimeException reason;
            if (e.getCause() instanceof TimeoutException) {
                reason = new RuntimeException("Timeout: ", e);
            } else {
                reason = new RuntimeException("Uncaught exception: ", e);
            }

            if (suppressExceptions) {
                return ErrorMessage.fromException(reason);
            } else
                throw reason;
        }
    }

    public RequestFuture executeAsync(Message.Request request) {
        return executeAsync(request, false);
    }

    public RequestFuture executeAsync(Message.Request request, boolean suppressExceptions) {
        try {
            if (!connected)
                start();

            request.setStreamId(getNextStreamId());
            RequestFuture requestFuture = new RequestFuture(request, 10000);
            dispatcher.add(requestFuture.callback);
            channel.writeAndFlush(request); // TODO: .addListener(futureListener());
            return requestFuture;
        } catch (Exception e) {
            RuntimeException reason;
            if (e.getCause() instanceof TimeoutException) {
                reason = new RuntimeException("Timeout: ", e);
            } else {
                reason = new RuntimeException("Uncaught exception: ", e);
            }

            if (suppressExceptions) {
                return null;
            } else
                throw reason;
        }
    }

    private int getNextStreamId() {
        if (requestCounter.get() == Integer.MAX_VALUE) {
            return requestCounter.getAndSet(0);
        }
        return requestCounter.incrementAndGet();
    }

    private GenericFutureListener futureListener() {
        return new GenericFutureListener() {

            @Override
            public void operationComplete(Future future) throws Exception {

            }
        };
    }

    private class ChannelFactory extends ChannelInitializer<SocketChannel> {

        private boolean init;

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            if (!init) {
                ch.pipeline()
                        .addLast(packetDecoder)
                        .addLast(packetEncoder)

                        .addLast(messageDecoder)
                        .addLast(messageEncoder)

                        .addLast(dispatcher);
                init = true;
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            StorageNodeClient.this.connected = false;
        }
    }

    private static class SimpleDispatcher extends SimpleChannelInboundHandler<Message.Response> {

        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<Message.Response>(true);

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Response message) throws Exception {
            try {
                responses.put(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }
    }

    private static class Dispatcher extends SimpleChannelInboundHandler<Message.Response> {

        public final BlockingQueue<Message.Response> responses = new SynchronousQueue<Message.Response>(true);

        final ConcurrentMap<Integer, RequestFuture.ResponseCallback> pending = new ConcurrentHashMap<Integer, RequestFuture.ResponseCallback>();

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Response message) throws Exception {
            RequestFuture.ResponseCallback callback = pending.remove(message.getStreamId());
            assert null != callback;
            callback.setSuccess(message);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            super.exceptionCaught(ctx, cause);
        }

        public void add(RequestFuture.ResponseCallback callback) {
            RequestFuture.ResponseCallback old = pending.put(callback.streamId, callback);
            assert null == old;

        }
    }
}
