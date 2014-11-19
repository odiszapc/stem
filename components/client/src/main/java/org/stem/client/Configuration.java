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

public class Configuration {

    private final Policies policies;

    private final SocketOpts socketOpts;
    private final ProtocolOpts protocolOpts;
    private final PoolingOpts poolingOpts;

    public Configuration() {
        this(new Policies(), new SocketOpts(), new ProtocolOpts(), new PoolingOpts());
    }

    public Configuration(Policies policies, SocketOpts socketOpts, ProtocolOpts protocolOpts, PoolingOpts poolingOpts) {
        this.policies = policies;
        this.socketOpts = socketOpts;
        this.protocolOpts = protocolOpts;
        this.poolingOpts = poolingOpts;
    }

    public Policies getPolicies() {
        return policies;
    }

    public SocketOpts getSocketOpts() {
        return socketOpts;
    }

    public ProtocolOpts getProtocolOpts() {
        return protocolOpts;
    }

    public PoolingOpts getPoolingOpts() {
        return poolingOpts;
    }
}
