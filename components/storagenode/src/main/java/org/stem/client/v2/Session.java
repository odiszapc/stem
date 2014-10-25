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

import com.datastax.driver.core.Host;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Session
{
    final StemCluster cluster;
    final ConcurrentMap<Host, ConnectionPool> pools;

    public Session(StemCluster cluster)
    {
        this.cluster = cluster;
        this.pools = new ConcurrentHashMap<>();
    }


    Configuration configuration() {
        return cluster.manager.configuration;
    }

    Connection.Factory connectionFactory() {
            return cluster.manager.connectionFactory;
        }

    ListeningExecutorService blockingExecutor() {
        return cluster.manager.blockingExecutor;
    }
}
