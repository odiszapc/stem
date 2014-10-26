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
import com.google.common.util.concurrent.Uninterruptibles;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.stem.exceptions.TimeoutException;
import org.stem.transport.Message;
import org.stem.transport.ops.ErrorMessage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RequestFuture extends AbstractFuture<Message.Response> {

    final ResponseCallback callback;
    private final Message.Request request;

    public Message.Request getRequest() {
        return request;
    }

    class ResponseCallback {
        final int streamId;
        public final HashedWheelTimer timer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("Timeouter-%d").build(),
                100, TimeUnit.MILLISECONDS);

        private final long startTime;
        private final int timeoutMs;

        public ResponseCallback(Message.Request request, int timeoutMs) {
            this.timeoutMs = timeoutMs;
            this.streamId = request.getStreamId();
            this.startTime = System.nanoTime();
            this.timer.newTimeout(newTimer(), this.timeoutMs, TimeUnit.MILLISECONDS);
        }

        private void onTimeout(long latency) {
            Throwable reason = new TimeoutException(String.format("Connection timeout: %s Ms > %s Ms", latency / 1000000, timeoutMs));
            setFailed(reason);
//            setException(new TimeoutException(
//                    String.format("Connection timeout: %s Ms > %s Ms", latency/1000000, timeoutMs)));
            // TODO: release streamId
        }

        private TimerTask newTimer() {
            return new TimerTask() {
                @Override
                public void run(Timeout timeout) throws Exception {
                    onTimeout(System.nanoTime() - startTime);
                }
            };
        }

        public void setSuccess(Message.Response msg) {
            //this.timer.stop(); // TODO: timer can't be stopped from thread it was not created from
            RequestFuture.this.set(msg);
        }

        public void setFailed(Throwable t) {
            setSuccess(ErrorMessage.fromException(t));
        }
    }

    public RequestFuture(Message.Request request, int timeoutMs) {
        this.request = request;
        this.callback = new ResponseCallback(request, timeoutMs);
    }

    public Message.Response getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}
