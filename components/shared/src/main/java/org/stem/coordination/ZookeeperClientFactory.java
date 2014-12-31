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

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ZookeeperClientFactory {

    private static Map<String, ZookeeperClient> registry = new ConcurrentHashMap<>();
    private static AtomicInteger counter = new AtomicInteger(0);

    public static ZookeeperClient newClient(String host, int port) throws ZooException {
        ZookeeperClient client = new ZookeeperClient(host, port);
        saveToRegistry(computeName(), client);
        return client;
    }

    public static ZookeeperClient newClient(String endpoint) throws ZooException {
        String[] split = StringUtils.split(endpoint, ':');
        assert 2 == split.length : "Invalid Zookeeper endpoint!";
        String host = split[0];
        int port = Integer.valueOf(split[1]);

        return newClient(host, port);
    }

    private static String computeName() {
        return "ZooClient-" + counter.incrementAndGet();
    }

    public static void closeAll() {

        for (ZookeeperClient client : registry.values()) {
            client.close();
        }

        registry.clear();
    }

    private static void saveToRegistry(String name, ZookeeperClient client) {
        registry.put(name, client);
    }
}
