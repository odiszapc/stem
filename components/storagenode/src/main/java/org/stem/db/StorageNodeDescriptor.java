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
import org.stem.utils.Utils;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.UUID;

public class StorageNodeDescriptor {

    private static final Logger logger = LoggerFactory.getLogger(StorageNodeDescriptor.class);

    private static Config config;
    private static final String STEM_CONFIG_PROPERTY = "stem.config";
    private static final String STEM_ID_PROPERTY = "stem.node.id";
    private static ClusterResponse.Cluster cluster; // This should be some of Topology or Cluster globals class, not from Response*
    private static MetaStoreClient metaStoreClient;
    public static UUID id;

    public static UUID getID() {
        return id;
    }

    static {
        loadConfig();
        loadOrCreateMeta();
    }

    public static void loadOrCreateMeta() {
        try {
            String path = loadSystemProperty(STEM_ID_PROPERTY);
            File file = new File(path);
            if (!file.exists()) {
                Utils.writeUuid(UUID.randomUUID(), path);
            }

            UUID uuid = Utils.readUuid(path);
            if (null == uuid)
                throw new Exception("Node id is null");

            id = uuid;
            logger.info("Node id={}", id);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
        String configPath = loadSystemProperty(STEM_CONFIG_PROPERTY);

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

    private static String loadSystemProperty(final String property) {
        String value = System.getProperty(property);
        if (null == value)
            throw new RuntimeException("System property \"" + property + "\" not set");
        return value;
    }

    public static String getClusterManagerEndpoint() {
        return config.cluster_manager_endpoint;
    }

    public static String[] getBlobMountPoints() {
        return config.blob_mount_points;
    }

    public static String getNodeListenAddress() { // TODO: change config parameter name according to this method's name
        return config.node_listen;
    }

    public static Integer getNodeListenPort() { // TODO: change config parameter name according to this method's name
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
        int vBuckets = StorageNodeDescriptor.cluster().getvBucketsNum(); // Hard binding: Layout -> cluster()
        Layout.getInstance().load(mountPoints, vBuckets);
    }

    public static void describeCluster() {
        cluster = ClusterService.describeAndInit();
        logger.info("Cluster loaded: {}", cluster);
        metaStoreClient = new MetaStoreClient(); // TODO: Metastore client? Here? Why?
        metaStoreClient.start();
    }

    public static void initStorageService() {
        StorageService.instance.init();
    }

    public static void detachLayout() {
        Layout.getInstance().detach();
    }

    public static ClusterResponse.Cluster cluster() {
        return cluster;
    }

    public static void joinCluster() throws Exception {
        ClusterService.instance.join();
        ClusterService.instance.startDataNotificator();
    }
}
