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

import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ReplicaResponseHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaResponseHandler.class);

    private final ConsistentResponseHandler context;
    final DefaultResultFuture future;
    private volatile Throwable cause = null;
    private final Host endpoint;

    private Message.Response response;


    public ReplicaResponseHandler(ConsistentResponseHandler context, DefaultResultFuture future) {
        this.context = context;
        this.future = future;
        endpoint = context.session.router.getHost(future.request()); // TODO: optimize
    }

    public void start() {
        try {
            response = Uninterruptibles.getUninterruptibly(future);
        } catch (ExecutionException e) {
            cause = e.getCause();
            logger.error("Error sending request {} to {}, {}", future.request(), endpoint, cause.getMessage());
        }
        finally {
            context.onRequestFinished(this);
        }
    }

    public Host getEndpoint() {
        return endpoint;
    }

    public Message.Response getResponse() {
        return response;
    }

    public boolean isSuccess() {
        return null == cause;
    }

    public Throwable getError() {
        return cause;
    }
}
