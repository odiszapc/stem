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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.stem.api.REST;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

public class Host {

    private InetSocketAddress address;

    enum State {ADDED, DOWN, SUSPECT, UP}

    volatile State state;

    final AtomicReference<ListenableFuture<?>> initialReconnectionAttempt = new AtomicReference<ListenableFuture<?>>(Futures.immediateFuture(null));

    final AtomicReference<ListenableFuture<?>> reconnectionAttempt = new AtomicReference<ListenableFuture<?>>();

    final ExecutionInfo defaultExecutionInfo;

    public Host(InetSocketAddress address) {
        this.address = address;
        this.state = State.ADDED;

        defaultExecutionInfo = new ExecutionInfo(this);
    }

    public boolean isUp() {
        return state == State.UP || state == State.SUSPECT;
    }

    public InetAddress getAddress() {
        return address.getAddress();
    }

    public InetSocketAddress getSocketAddress() {
        return address;
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }
}
