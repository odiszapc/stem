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
import org.stem.exceptions.ConnectionBusyException;
import org.stem.exceptions.ConnectionException;
import org.stem.transport.ops.ErrorMessage;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RequestHandler implements Connection.ResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private final Session session;
    private final Callback callback;

    private volatile Host current;
    private volatile ConnectionPool currentPool;

    private volatile Map<InetSocketAddress, Throwable> errors;

    private volatile boolean isCanceled;
    private volatile Connection.ResponseHandler connectionHandler;

    private final Timer.Context timerContext;
    private final long startTime;

    public RequestHandler(Session session, Callback callback) {

        this.session = session;
        this.callback = callback;

        callback.register(this);

        this.timerContext = null; // TODO: metrics support
        this.startTime = System.nanoTime();
    }

    private boolean query(Host host) {
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
            logError(host.getSocketAddress(), new StemClientException("Timeout while trying to acquire available connection (you may want to increase the number of per-host connections)"));
            return false;
        } catch (RuntimeException e) {
            if (connection != null)
                connection.release();
            logger.error("Unexpected error while querying " + host.getAddress(), e);
            logError(host.getSocketAddress(), e);
            return false;
        }
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
        Host queriedHost = current;
        try {
            if (connection instanceof PooledConnection)
                ((PooledConnection) connection).release();

            switch (response.type) {
                case Type.RESULT:
                    setFinalResult(connection, response);
                case Type.ERROR:
                    ErrorMessage err = (ErrorMessage) response;
                    StemClientException e = err.error;
            }
        }
    }

    private void setFinalResult(Connection connection, Message.Response response) {
        try {
            if (timerContext != null)
                timerContext.stop();

            ExecutionInfo info = current.defaultExecutionInfo;

            callback.onSet(connection, response, info, System.nanoTime() - startTime);
        } catch (Exception e) {
            callback.onException(connection, new StemClientInternalError("Unexpected exception while setting final result from " + response, e), System.nanoTime() - startTime);
        }
    }

    @Override
    public void onException(Connection connection, Exception exception, long latency) {

    }

    @Override
    public void onTimeout(Connection connection, long latency) {

    }

    public void sendRequest() {

    }

    public void cancel() {
        isCanceled = true;
        if (connectionHandler != null)
            connectionHandler.cancelHandler();
    }

    interface Callback extends Connection.ResponseCallback {

        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency);

        public void register(RequestHandler handler);
    }
}
