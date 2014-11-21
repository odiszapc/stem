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

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.exceptions.ClientTransportException;
import org.stem.exceptions.ConnectionBusyException;
import org.stem.exceptions.ConnectionException;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Connection {

    private static final Logger logger = LoggerFactory.getLogger(Connection.class);

    private final String name;
    public final InetSocketAddress address;
    private final Factory factory;
    private final Channel channel;
    private final Dispatcher dispatcher;
    private volatile boolean isInitialized;
    private volatile boolean isDefunct;
    private final AtomicReference<ConnectionCloseFuture> closeFutureRef = new AtomicReference<>();

    public final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicInteger writeCounter = new AtomicInteger(0);

    private final Object terminationLock = new Object();

    public Connection(String name, InetSocketAddress address, Factory factory) throws ConnectionException {
        this.name = name;
        this.address = address;
        this.factory = factory;
        dispatcher = new Dispatcher();

        Bootstrap bootstrap = factory.createNewBootstrap();
        bootstrap.handler(new ChannelHandler(this));

        ChannelFuture future = bootstrap.connect(address);
        writeCounter.incrementAndGet();
        try {
            this.channel = future.awaitUninterruptibly().channel();
            if (!future.isSuccess()) {
                if (logger.isDebugEnabled())
                    logger.debug(String.format("%s Error connecting to %s%s", this, address, extractMessage(future.cause())));
                throw defunct(new ClientTransportException(address, "Can't connect", future.cause()));
            }
        }
        finally {
            writeCounter.decrementAndGet();
        }

        logger.trace("{} Connection opened successfully", this);
        isInitialized = true;
    }

    private static String extractMessage(Throwable t) {
        if (t == null)
            return "";
        String msg = t.getMessage() == null || t.getMessage().isEmpty()
                ? t.toString()
                : t.getMessage();
        return " (" + msg + ')';
    }

    public int maxAvailableStreams() {
        return dispatcher.streamIdPool.maxAvailableStreams();
    }

    <E extends Exception> E defunct(E e) {
        if (logger.isDebugEnabled())
            logger.debug("Defuncting connection to " + address, e);
        isDefunct = true;

        ConnectionException ce = e instanceof ConnectionException
                ? (ConnectionException) e
                : new ConnectionException(address, "Connection problem", e);

        Host host = factory.manager.metadata.getHost(address);
        if (host != null) {
            boolean isReconnectionAttempt = (host.state == Host.State.DOWN || host.state == Host.State.SUSPECT)
                    && !(this instanceof PooledConnection);
            if (!isReconnectionAttempt) {
                boolean isDown = factory.manager.signalConnectionFailure(host, ce, host.wasJustAdded(), isInitialized);
                notifyOwnerWhenDefunct(isDown);
            }
        }

        closeAsync().force();

        return e;
    }

    public boolean isDefunct() {
        return isDefunct;
    }

    public Future write(Message.Request request) throws ConnectionBusyException, ConnectionException {
        Future future = new Future(request);
        write(future);
        return future;
    }

    public ResponseHandler write(ResponseCallback callback) throws ConnectionBusyException, ConnectionException {
        Message.Request request = callback.request();
        ResponseHandler responseHandler = new ResponseHandler(this, callback);
        dispatcher.addHandler(responseHandler);
        request.setStreamId(responseHandler.streamId);

        if (isDefunct) {
            dispatcher.removeHandler(responseHandler.streamId, true);
            throw new ConnectionException(address, "Writing on deactivated connection");
        }

        if (isClosed()) {
            dispatcher.removeHandler(responseHandler.streamId, true);
            throw new ConnectionException(address, "Connection has been closed");
        }

        logger.trace("{} writing request {}", this, request);
        writeCounter.incrementAndGet();
        channel.write(request).addListener(writeHandler(request, responseHandler));
        return responseHandler;
    }

    private ChannelFutureListener writeHandler(final Message.Request request, final ResponseHandler handler) {
        return new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                writeCounter.decrementAndGet();
                if (!future.isSuccess()) {
                    logger.debug("{} Error writing request {}", Connection.this, request);
                    dispatcher.removeHandler(handler.streamId, true);

                    ConnectionException e;
                    if (future.cause() instanceof ClosedChannelException) {
                        e = new ClientTransportException(address, "Error writing to closed channel");
                    } else {
                        e = new ClientTransportException(address, "Error writing", future.cause());
                    }
                    handler.callback.onException(Connection.this, defunct(e), System.nanoTime() - handler.startedAt);
                } else {
                    logger.trace("{} request sent successfully", Connection.this);
                }
            }
        };
    }

    public CloseFuture closeAsync() {
        ConnectionCloseFuture future = new ConnectionCloseFuture();
        if (!closeFutureRef.compareAndSet(null, future)) {
            return closeFutureRef.get();
        }

        logger.debug("{} closing connection", this);

        boolean terminated = terminate(false);

        // TODO:
        //if (!terminated)
        //factory.reaper.register(this);

        if (dispatcher.pending.isEmpty())
            future.force();
        return future;
    }

    private boolean terminate(boolean evenIfPending) {
        assert isClosed();
        ConnectionCloseFuture future = closeFutureRef.get();
        if (future.isDone()) {
            logger.debug("{} has already terminated", this);
            return true;
        } else {
            synchronized (terminationLock) {
                if (evenIfPending || dispatcher.pending.isEmpty()) {
                    future.force();
                    // Bug ?
                    return true;
                } else {
                    logger.debug("Not terminating {}: there are still pending requests", this);
                    return false;
                }
            }
        }
    }

    public boolean isClosed() {
        return closeFutureRef.get() != null;
    }

    protected void notifyOwnerWhenDefunct(boolean hostIsDown) {
    }

    @Override
    public String toString() {
        return String.format("Connection[%s, inFlight=%d, closed=%b]", name, inFlight.get(), isClosed());
    }

    /**
     *
     */
    private class ConnectionCloseFuture extends CloseFuture {

        @Override
        public ConnectionCloseFuture force() {
            if (null == channel) {
                set(null);
                return this;
            }

            ChannelFuture future = channel.close();
            future.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (future.cause() != null)
                        ConnectionCloseFuture.this.setException(future.cause());
                    else
                        ConnectionCloseFuture.this.set(null);
                }
            });
            return this;
        }
    }

    /**
     *
     */
    public static class ChannelHandler extends ChannelInitializer<SocketChannel> {

        private Connection connection;

        public ChannelHandler(Connection connection) {
            this.connection = connection;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("packetDecoder", new Frame.Decoder());
            pipeline.addLast("packetEncoder", new Frame.Encoder());

            pipeline.addLast("messageDecoder", new Message.ProtocolDecoder());
            pipeline.addLast("messageEncoder", new Message.ProtocolEncoder());

            pipeline.addLast("dispatcher", connection.dispatcher);
        }
    }

    /**
     *
     */
    public static class Factory {

        EventLoopGroup workerGroup = new NioEventLoopGroup();

        private StemCluster.Manager manager;
        private final Configuration configuration;
        public final Timer timer = new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("Timeouter-%d").build());
        private volatile boolean isShutdown;
        private InetSocketAddress address;

        private final ConcurrentMap<Host, AtomicInteger> idGenerators = new ConcurrentHashMap<>();

        public Factory(StemCluster.Manager manager, Configuration configuration) {
            this.manager = manager;
            this.configuration = configuration;
        }

        public Connection open(Host host) throws ConnectionException {
            address = host.getSocketAddress();
            if (isShutdown)
                throw new ConnectionException(address, "Connection factory is shut down");

            String name = address.toString() + '-' + getIdGenerator(host).getAndIncrement();
            return new Connection(name, address, this);
        }

        public PooledConnection open(ConnectionPool pool) throws ConnectionException {
            InetSocketAddress address = pool.host.getSocketAddress();

            if (isShutdown)
                throw new ConnectionException(address, "Connection factory is shut down");

            String name = address.toString() + '-' + getIdGenerator(pool.host).getAndIncrement();
            return new PooledConnection(name, address, this, pool);
        }

        public Bootstrap createNewBootstrap() {
            SocketOpts opt = this.configuration.getSocketOpts();

            Bootstrap bootstrap = new Bootstrap();

            bootstrap
                    .group(workerGroup)
                    .channel(NioSocketChannel.class);

            bootstrap
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, opt.getConnectTimeoutMs());

            Boolean keepAlive = opt.getKeepAlive();
            if (null != keepAlive)
                bootstrap.option(ChannelOption.SO_KEEPALIVE, keepAlive);

            Boolean reuseAddress = opt.getReuseAddress();
            if (null != reuseAddress)
                bootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);

            Integer soLinger = opt.getSoLinger();
            if (null != soLinger)
                bootstrap.option(ChannelOption.SO_LINGER, soLinger);

            Boolean topNoDelay = opt.getTcpNoDelay();
            if (null != topNoDelay)
                bootstrap.option(ChannelOption.TCP_NODELAY, topNoDelay);

            Integer receiveBufferSize = opt.getReceiveBufferSize();
            if (null != receiveBufferSize)
                bootstrap.option(ChannelOption.SO_RCVBUF, receiveBufferSize);

            Integer sendBufferSize = opt.getSendBufferSize();
            if (null != sendBufferSize)
                bootstrap.option(ChannelOption.SO_SNDBUF, sendBufferSize);

            return bootstrap;
        }

        public void shutdown() {
            isShutdown = true;
            workerGroup.shutdownGracefully().awaitUninterruptibly();
            timer.stop();
        }

        public int getReadTimeoutMs() {
            return configuration.getSocketOpts().getReadTimeoutMs();
        }

        private AtomicInteger getIdGenerator(Host host) {
            AtomicInteger g = idGenerators.get(host);
            if (g == null) {
                g = new AtomicInteger(1);
                AtomicInteger old = idGenerators.putIfAbsent(host, g);
                if (old != null)
                    g = old;
            }
            return g;
        }
    }

    /**
     *
     */
    private class Dispatcher extends SimpleChannelInboundHandler<Message.Response> {

        private final ConcurrentMap<Integer, ResponseHandler> pending = new ConcurrentHashMap<Integer, ResponseHandler>();
        public final StreamIdPool streamIdPool = new StreamIdPool();


        public void addHandler(ResponseHandler handler) {
            ResponseHandler oldHandler = pending.put(handler.streamId, handler);
            assert null == oldHandler;
        }

        public void removeHandler(int streamId, boolean releaseStreamId) {
            ResponseHandler removed = pending.remove(streamId);
            if (null != removed) {
                removed.cancelTimeout();
            }

            if (releaseStreamId) {
                streamIdPool.release(streamId);
            }

            if (isClosed())
                terminate(false);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Message.Response response) throws Exception {
            int streamId = response.getStreamId();
            logger.trace("{} received: {}", Connection.this, response);

            if (streamId < 0) {
                // TODO: Handle server-side streams
                // factory.defaultHandler.handle(response);
                return;
            }

            ResponseHandler handler = pending.remove(streamId);
            streamIdPool.release(streamId);
            if (null == handler) {
                streamIdPool.unmark();
                if (logger.isDebugEnabled())
                    logger.debug("{} Response received on stream {} but no handler set anymore (either the request has "
                            + "timed out or it was closed due to another error). Received message is {}", Connection.this, streamId, asDebugString(response));
                return;
            }
            handler.cancelTimeout();
            handler.callback.onSet(Connection.this, response, System.nanoTime() - handler.startedAt);

            if (isClosed())
                terminate(false);
        }

        private String asDebugString(Object obj) {
            if (obj == null)
                return "null";

            String msg = obj.toString();
            if (msg.length() < 500)
                return msg;

            return msg.substring(0, 500) + "... [message of size " + msg.length() + " truncated]";
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
            if (logger.isDebugEnabled())
                logger.debug(String.format("%s connection error", Connection.this), e.getCause());

            if (writeCounter.get() > 0)
                return;

            defunct(new ClientTransportException(address, String.format("Unexpected exception %s", e.getCause()), e.getCause()));
        }

        public void dropAllHandlers(ConnectionException e) {
            Iterator<ResponseHandler> it = pending.values().iterator();
            while (it.hasNext()) {
                ResponseHandler handler = it.next();
                handler.cancelTimeout();
                handler.callback.onException(Connection.this, e, System.nanoTime() - handler.startedAt);
                it.remove();
            }
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            // TODO: Should we do some work here?
            logger.trace("Channel unregistered");
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            ConnectionException closeException = new ClientTransportException(address, "Channel has been closed");

            if (!isInitialized || isClosed()) {
                dropAllHandlers(closeException);
                Connection.this.closeAsync().force();
            } else {
                defunct(closeException);
            }

        }
    }

    /**
     *
     */
    static class Future extends AbstractFuture<Message.Response> implements RequestHandler.Callback {

        private final Message.Request request;
        private volatile InetSocketAddress address;

        Future(Message.Request request) {
            this.request = request;
        }

        @Override
        public void register(RequestHandler handler) {

        }

        @Override
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency) {
            onSet(connection, response, latency);
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency) {
            this.address = connection.address;
            super.set(response); // AbstractFuture
        }

        @Override
        public Message.Request request() {
            return null;
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency) {
            super.setException(exception); // AbstractFuture
        }

        @Override
        public void onTimeout(Connection connection, long latency) {
            assert connection != null;
            this.address = connection.address;
            super.setException(new ConnectionException(connection.address, "Operation timed out"));
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    /**
     *
     */
    interface ResponseCallback {

        public Message.Request request();
        public void onSet(Connection connection, Message.Response response, long latency);
        public void onException(Connection connection, Exception exception, long latency);
        public void onTimeout(Connection connection, long latency);
    }

    /**
     *
     */
    static class ResponseHandler {

        public final Connection connection;
        public final ResponseCallback callback;
        public final int streamId;

        private final Timeout timeout;
        private final long startedAt;

        ResponseHandler(Connection connection, ResponseCallback callback) throws ConnectionBusyException {
            this.connection = connection;
            this.callback = callback;
            this.streamId = connection.dispatcher.streamIdPool.borrow();

            long timeoutMs = connection.factory.getReadTimeoutMs();
            this.timeout = connection.factory.timer.newTimeout(newTimeoutTask(), timeoutMs, TimeUnit.MILLISECONDS);

            this.startedAt = System.nanoTime();
        }

        /**
         * Invoked by Dispatcher
         */
        void cancelTimeout() {
            if (null != timeout)
                timeout.cancel();
        }

        public void cancelHandler() {
            connection.dispatcher.removeHandler(streamId, false);
            if (connection instanceof PooledConnection)
                ((PooledConnection) connection).release();
        }

        private TimerTask newTimeoutTask() {
            return new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {

                }
            };
        }
    }
}
