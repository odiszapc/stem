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
import org.stem.transport.Message;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DefaultResponseFuture extends AbstractFuture<Message.Response> implements ResponseFuture, RequestHandler.Callback {
    private final Session session;
    private final Message.Request request;

    public DefaultResponseFuture(Session session, Message.Request request) {

        this.session = session;
        this.request = request;
    }

    @Override
    public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency) {

    }

    @Override
    public void register(RequestHandler handler) {

    }

    @Override
    public Message.Request request() {
        return null;
    }

    @Override
    public void onSet(Connection connection, Message.Response response, long latency) {

    }

    @Override
    public void onException(Connection connection, Exception exception, long latency) {

    }

    @Override
    public void onTimeout(Connection connection, long latency) {

    }

    @Override
    public Message.Response getUninterruptibly() {


    }

    @Override
    public Message.Response getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        return null;
    }
}
