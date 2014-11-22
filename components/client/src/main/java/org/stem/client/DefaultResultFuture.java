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
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DefaultResultFuture extends AbstractFuture<Message.Response> implements ResponseFuture, RequestHandler.Callback {

    private static final Logger logger = LoggerFactory.getLogger(ResponseFuture.class);

    private final Session session;
    private final Message.Request request;
    private volatile RequestHandler handler;

    public DefaultResultFuture(Session session, Message.Request request) {
        this.session = session;
        this.request = request;
    }

    @Override
    public void register(RequestHandler handler) {
        this.handler = handler;
    }

    @Override
    public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency) {
        try {
            switch (response.type) {
                case RESULT:
                    Responses.Result rm = (Responses.Result) response;
                    switch (rm.kind) {
                        default:
                            set(rm);
                    }
                    break;
                case ERROR:
                    setException(((Responses.Error) response).asException(connection.address));
                    break;
                default:
                    connection.defunct(new ConnectionException(connection.address, String.format("Got unexpected %s response", response.type)));
                    setException(new ClientInternalError(String.format("Got unexpected %s response from %s", response.type, connection.address)));
                    break;
            }
        } catch (RuntimeException e) {
            setException(new ClientInternalError("Unexpected error while processing response from " + connection.address, e));
        }
    }


    @Override
    public void onSet(Connection connection, Message.Response response, long latency) {
        onSet(connection, response, null, latency);
    }

    @Override
    public Message.Request request() {
        return request;
    }


    @Override
    public void onException(Connection connection, Exception exception, long latency) {
        setException(exception);
    }

    @Override
    public void onTimeout(Connection connection, long latency) {
        setException(new ConnectionException(connection.address, "Operation timed out"));
    }

    @Override
    public Message.Response getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    @Override
    public Message.Response getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(this, timeout, unit);
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!super.cancel(mayInterruptIfRunning))
            return false;

        handler.cancel();
        return true;
    }

    static RuntimeException extractCauseFromExecutionException(ExecutionException e) {
        if (e.getCause() instanceof ClientException)
            throw ((ClientException) e.getCause()).copy();
        else
            throw new ClientInternalError("Unexpected exception thrown", e.getCause());
    }
}
