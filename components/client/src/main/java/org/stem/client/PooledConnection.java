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

import org.stem.exceptions.ConnectionException;

import java.net.InetSocketAddress;

public class PooledConnection extends Connection {

    private ConnectionPool pool;

    public PooledConnection(String name, InetSocketAddress address, Factory factory, ConnectionPool pool) throws ConnectionException {
        super(name, address, factory);
        this.pool = pool;
    }

    public void release() {
        pool.returnConnection(this);
    }

    @Override
    protected void notifyOwnerWhenDefunct(boolean hostIsDown) {
        if (pool == null)
            return;

        if (hostIsDown) {
            pool.closeAsync().force();
        } else {
            pool.replace(this);
        }
    }
}
