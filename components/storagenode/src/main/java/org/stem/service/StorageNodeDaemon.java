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

package org.stem.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.db.StorageNodeDescriptor;
import org.stem.net.Server;
import org.stem.streaming.StreamManager;

public class StorageNodeDaemon {

    public static final StorageNodeDaemon instance = new StorageNodeDaemon();

    private static final Logger logger = LoggerFactory.getLogger(StorageNodeDaemon.class);

    private Server server;

    public static void main(String[] args) {
        instance.start();
    }

    public void start() {
        try {
            setup();
            startService();
        } catch (Throwable e) {
            e.printStackTrace();
            System.out.println("Error occurred during startup: " + e.getMessage());
            System.exit(3);
        }
    }

    private void setup() {
        logger.info("Starting Storage Node daemon");
        logger.info("Heap size: {}/{}", Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory());
        logger.info("Classpath: {}", System.getProperty("java.class.path"));

        try {
            if (StorageNodeDescriptor.getAutoAllocate()) {
                logger.warn("Auto-allocation of fat files is turned on.");
            }

            StorageNodeDescriptor.describeCluster(); // Describe cluster
            StorageNodeDescriptor.loadLayout(); // init mount points with vBuckets parameter
            StorageNodeDescriptor.initStorageService(); // activate WriteControllers with mount points
            StorageNodeDescriptor.joinCluster();
            StreamManager.instance.listenForSessions();

        } catch (Exception e) {
            logger.error("Error during initialization", e);
            System.exit(100);
        }

        server = new Server(
                StorageNodeDescriptor.getNodeListen(),
                StorageNodeDescriptor.getNodePort());
    }

    private void startService() {
        server.start();
    }

    public void stop() {
        server.stop();
        StorageNodeDescriptor.detachLayout();
    }
}
