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

package org.stem.coordination;

import com.google.common.util.concurrent.Striped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import static org.stem.utils.Utils.getHost;
import static org.stem.utils.Utils.getPort;

public class ZookeeperFactoryCached {

    private static final Logger logger = LoggerFactory.getLogger(ZookeeperFactoryCached.class);

    public static final String NAME_PATTERN = "zookeeper-%s";

    private static Striped<Lock> poolCreationLocks = Striped.lock(2);

    private static Map<String, ZookeeperClient> pool = new ConcurrentHashMap<>();
    private static AtomicInteger counter = new AtomicInteger(1);

    public static ZookeeperClient newClient(String endpoint) throws ZooException {
        return newClient(endpoint, false);
    }

    public static ZookeeperClient newClient(String endpoint, boolean force) throws ZooException {
        String name = force ? computeName() : computeName(endpoint);
        return newClient(name, endpoint);
    }

    private static ZookeeperClient newClient(String name, String endpoint) throws ZooException {
        return newClient(name, getHost(endpoint), getPort(endpoint));
    }

    private static ZookeeperClient newClient(String name, String host, int port) throws ZooException {
        return createCached(name, host, port);
    }

    private static ZookeeperClient createCached(String name, String host, int port) throws ZooException {
        Lock l = poolCreationLocks.get(name);
        l.lock();
        try {
            ZookeeperClient candidate = pool.get(name);
            if (null == candidate) {
                logger.info("Creating new client: {}", name);
                candidate = new ZookeeperClient(host, port);
                pool.put(name, candidate);
            } else {
                logger.info("Utilize existing instance: {}", name);
            }
            return candidate;
        }
        finally {
            l.unlock();
        }
    }

    private static String computeName(String endpoint) {
        return String.format(NAME_PATTERN, endpoint);
    }

    private static String computeName() {
        return String.format(NAME_PATTERN, counter.incrementAndGet());
    }

    public static void closeAll() {
        for (ZookeeperClient client : pool.values()) {
            client.close();
        }

        pool.clear();
    }

    private static void saveToRegistry(String name, ZookeeperClient client) {
        pool.put(name, client);
    }
}
