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

import org.stem.transport.Message;

public class RequestHandler implements Connection.ResponseCallback {
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

    interface Callback extends Connection.ResponseCallback {
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency);

        public void register(RequestHandler handler);
    }
}
