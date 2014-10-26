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

package org.stem.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.api.response.ClusterResponse;
import org.stem.client.MetaStoreClient;
import org.stem.config.Config;
import org.stem.service.ClusterService;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class StorageNodeDescriptor {

    private static final Logger logger = LoggerFactory.getLogger(StorageNodeDescriptor.class);
    private static Config config;
    private static final String STEM_CONFIG_PROPERTY = "stem.config";
    private static ClusterResponse.Cluster cluster;
    private static MetaStoreClient metaStoreClient;

    static {
        loadConfig();
    }

    public static void loadConfig() {
        URL url = getConfigUrl();
        logger.info("Loading settings from " + url);

        InputStream stream;
        try {
            stream = url.openStream();
        } catch (IOException e) {
            throw new AssertionError(e);
        }

        Constructor constructor = new Constructor(Config.class);
        Yaml yaml = new Yaml(constructor);
        config = (Config) yaml.load(stream);
    }

    static URL getConfigUrl() {
        String configPath = System.getProperty(STEM_CONFIG_PROPERTY);
        if (null == configPath)
            throw new RuntimeException("System property \"" + STEM_CONFIG_PROPERTY + "\" not set");

        URL url;

        try {
            File file = new File(configPath);
            url = file.toURI().toURL();
            url.openStream().close();
        } catch (Exception e) {
            throw new RuntimeException("Cannot load " + configPath);
        }
        return url;
    }

    public static String getClusterManagerEndpoint() {
        return config.cluster_manager_endpoint;
    }

    public static String[] getBlobMountPoints() {
        return config.blob_mount_points;
    }

    public static String getNodeListen() {
        return config.node_listen;
    }

    public static Integer getNodePort() {
        return config.node_port;
    }

    public static Integer getFatFileSizeInMb() {
        return config.fat_file_size_in_mb;
    }

    public static boolean getMarkOnAllocate() {
        return config.mark_on_allocate;
    }

    public static boolean getAutoAllocate() {
        return config.auto_allocate;
    }

    public static float getCompactionThreshold() {
        return config.compaction_threshold;
    }

    public static Integer getMaxAllocationInMb() {
        return null == config.max_space_allocation_in_mb
                ? 0
                : config.max_space_allocation_in_mb;
    }

    public static MetaStoreClient getMetaStoreClient() {
        return metaStoreClient;
    }

    public static void loadLayout() throws IOException {
        String[] mountPoints = getBlobMountPoints();
        int vBuckets = StorageNodeDescriptor.getCluster().getvBucketsNum(); // Hard binding: Layout -> getCluster()
        Layout.getInstance().load(mountPoints, vBuckets);
    }

    public static void describeCluster() {
        cluster = ClusterService.instance.describeCluster();
        logger.info("Cluster description: {}", cluster);
        metaStoreClient = new MetaStoreClient();
        metaStoreClient.start();
    }

    public static void initStorageService() {
        StorageService.instance.init();
    }

    public static void detachLayout() {
        Layout.getInstance().detach();
    }

    public static ClusterResponse.Cluster getCluster() {
        return cluster;
    }

    public static void joinCluster() {
        ClusterService.instance.join();
        ClusterService.instance.startDataNotificator();
    }
}
