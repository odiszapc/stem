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

public class Policies {

    private static final ReconnectionPolicy DEFAULT_RECONNECTION_POLICY = new ConstantReconnectionPolicy(1000);

    private final ReconnectionPolicy reconnectionPolicy;

    public Policies() {
        this(DEFAULT_RECONNECTION_POLICY);
    }

    public Policies(ReconnectionPolicy reconnectionPolicy) {
        this.reconnectionPolicy = reconnectionPolicy;
    }

    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    public static ReconnectionPolicy defaultReconnectionPolicy() {
        return DEFAULT_RECONNECTION_POLICY;
    }
}
