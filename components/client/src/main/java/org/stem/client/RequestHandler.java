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

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RequestHandler implements Connection.ResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private final Session session;
    private final Callback callback;
    private final Message.Request request;

    private volatile Host current;
    private volatile ConnectionPool currentPool;

    private volatile Map<InetSocketAddress, Throwable> errors;

    private volatile boolean isCanceled;
    private volatile Connection.ResponseHandler connectionHandler;

    private final Timer.Context timerContext;
    private final long startTime;

    public RequestHandler(Session session, Callback callback, Message.Request request) {
        this.session = session;
        this.callback = callback;
        this.request = request;

        callback.register(this);

        this.timerContext = null; // TODO: metrics support
        this.startTime = System.nanoTime();
    }

    private void logError(InetSocketAddress address, Throwable exception) {
        logger.debug("Error querying {}, trying next host (error is: {})", address, exception.toString());
        if (errors == null)
            errors = new HashMap<InetSocketAddress, Throwable>();
        errors.put(address, exception);
    }

    @Override
    public Message.Request request() {
        Message.Request request = callback.request();
        return request;
    }

    @Override
    public void onSet(Connection connection, Message.Response response, long latency) {
        try {
            if (connection instanceof PooledConnection)
                ((PooledConnection) connection).release();

            switch (response.type) {
                case RESULT:
                    setFinalResult(connection, response);
                    break;
                case ERROR:
                    Responses.Error err = (Responses.Error) response;
                    switch (err.code) {
                        case SERVER_ERROR:
                            logger.warn("{} replied with server error ({}), trying next host.", connection.address, err.message);
                            ClientException exception = new ClientException(
                                    String.format("Host %s replied with server error: %s", connection.address, err.message));
                            logError(connection.address, exception);
                            connection.defunct(exception);
                            setFinalException(connection, exception);
                            return;
                        default:
                            break;
                    }
                    setFinalResult(connection, response);
                    break;
                default:
                    setFinalResult(connection, response);
                    break;
            }
        } catch (Exception e) {
            setFinalException(connection, e);
        }
    }

    private void setFinalResult(Connection connection, Message.Response response) {
        try {
            if (timerContext != null)
                timerContext.stop();

            ExecutionInfo info = current.defaultExecutionInfo;

            callback.onSet(connection, response, info, System.nanoTime() - startTime);
        } catch (Exception e) {
            callback.onException(connection, new ClientInternalError("Unexpected exception while setting final result from " + response, e), System.nanoTime() - startTime);
        }
    }

    @Override
    public void onException(Connection connection, Exception exception, long latency) {
        try {
            if (connection instanceof PooledConnection)
                ((PooledConnection) connection).release();
            if (exception instanceof ConnectionException) {
                ConnectionException ce = (ConnectionException) exception;
                logError(ce.address, ce);
                return;
            }
            setFinalException(connection, exception);
        } catch (Exception e) {
            setFinalException(null, new ClientInternalError("An unexpected error happened while handling exception " + exception, e));
        }
    }

    @Override
    public void onTimeout(Connection connection, long latency) {
        try {
            ClientException timeoutException = new ClientException("Timed out waiting for server response");
            connection.defunct(timeoutException); // TODO: and what?
            logError(connection.address, timeoutException);
            setFinalException(connection, timeoutException);
        } catch (Exception e) {
            setFinalException(null, new ClientInternalError("An unexpected error happened while handling timeout", e));
        }
    }

    public void sendRequest() {
        try {
            Host host = session.router.getHost(request);
            if (query(host))
                return;

            setFinalException(null, new NoHostAvailableException("No hosts available"));
        } catch (Exception e) {
            setFinalException(null, new ClientInternalError("An unexpected error occurred while sending request", e));
        }
    }

    private boolean query(Host host) {
        if (null == host)
            return false;
        logger.trace("Querying storage node {}", host);

        currentPool = session.pools.get(host);
        if (currentPool == null || currentPool.isClosed())
            return false;

        PooledConnection connection = null;
        try {
            connection = currentPool.borrowConnection(session.configuration().getSocketOpts().getConnectTimeoutMs(), TimeUnit.MILLISECONDS);
            current = host;
            connectionHandler = connection.write(this);
            return true;
        } catch (ConnectionException e) {
            if (null != connection)
                connection.release();
            logError(host.getSocketAddress(), e);
            return false;
        } catch (ConnectionBusyException e) {
            if (connection != null)
                connection.release();
            logError(host.getSocketAddress(), e);
            return false;
        } catch (TimeoutException e) {
            logError(host.getSocketAddress(), new ClientException("Timeout while trying to acquire available connection (you may want to increase the number of per-host connections)"));
            return false;
        } catch (RuntimeException e) {
            if (connection != null)
                connection.release();
            logger.error("Unexpected error while querying " + host.getAddress(), e);
            logError(host.getSocketAddress(), e);
            return false;
        }
    }

    public void cancel() {
        isCanceled = true;
        if (connectionHandler != null)
            connectionHandler.cancelHandler();
    }

    private void setFinalException(Connection connection, Exception exception) {
        try {
            if (timerContext != null)
                timerContext.stop();
        }
        finally {
            callback.onException(connection, exception, System.nanoTime() - startTime);
        }
    }

    interface Callback extends Connection.ResponseCallback {

        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency);
        public void register(RequestHandler handler);
    }
}
