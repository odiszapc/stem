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

public class PoolingOpts
{
    private static final int START_CONNECTIONS_PER_HOST = 1;
    private static final int MAX_CONNECTIONS_PER_HOST = 5;
    private int startConnectionsPerHost = START_CONNECTIONS_PER_HOST;
    private int maxConnectionsPerHost = MAX_CONNECTIONS_PER_HOST;

    public int getStartConnectionsPerHost()
    {
        return startConnectionsPerHost;
    }

    public synchronized PoolingOpts setStartConnectionsPerHost(int startConnectionsPerHost)
    {
        this.startConnectionsPerHost = startConnectionsPerHost;
        return this;
    }

    public int getMaxConnectionsPerHost()
    {
        return maxConnectionsPerHost;
    }

    public synchronized PoolingOpts setMaxConnectionsPerHost(int maxConnectionsPerHost)
    {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
        return this;
    }
}
