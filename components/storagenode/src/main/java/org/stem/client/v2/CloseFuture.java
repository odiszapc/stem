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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import java.util.List;

public abstract class CloseFuture extends AbstractFuture<Void> {

    CloseFuture() {
    }

    static CloseFuture immediateFuture() {
        CloseFuture future = new CloseFuture() {
            @Override
            public CloseFuture force() {
                return this;
            }
        };
        future.set(null);
        return future;
    }

    public abstract CloseFuture force();

    static class Forwarding extends CloseFuture {

        private final List<CloseFuture> futures;

        Forwarding(List<CloseFuture> futures) {
            this.futures = futures;

            Futures.addCallback(Futures.allAsList(futures), new FutureCallback<List<Void>>() {
                public void onFailure(Throwable t) {
                    Forwarding.this.setException(t);
                }

                public void onSuccess(List<Void> v) {
                    Forwarding.this.onFuturesDone();
                }
            });
        }

        @Override
        public CloseFuture force() {
            for (CloseFuture future : futures)
                future.force();
            return this;
        }

        protected void onFuturesDone() {
            set(null);
        }
    }
}
