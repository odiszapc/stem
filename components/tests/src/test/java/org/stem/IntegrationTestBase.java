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

package org.stem;

import com.datastax.driver.core.Session;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.stem.api.ClusterManagerClient;
import org.stem.api.response.StemResponse;
import org.stem.client.Blob;
import org.stem.client.StemCluster;
import org.stem.coordination.ZookeeperFactoryCached;
import org.stem.db.Layout;
import org.stem.db.MountPoint;
import org.stem.db.StorageNodeDescriptor;
import org.stem.db.StorageService;
import org.stem.domain.topology.Partitioner;
import org.stem.service.StorageNodeDaemon;
import org.stem.transport.ops.WriteBlobMessage;
import org.stem.utils.TestUtils;
import org.stem.utils.YamlConfigurator;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IntegrationTestBase {

    ClusterManagerDaemon daemon;
    private TestingServer zookeeperInstance;
    protected ClusterManagerClient clusterManagerClient;
    protected Session cassandraTestSession;

    StemCluster client;
    org.stem.client.Session session;

    protected String clusterManagerAddress() {
        return "http://127.0.0.1:9997";
    }

    @Before
    public void setUp() throws Exception {
        TestUtils.cleanupTempDir();
        TestUtils.createTempDir();

        setupEnvironment();
        cleanupDataDirectories();

        startCassandraEmbedded();
        loadSchema();

        startZookeeperEmbedded();
        startClusterManagerEmbedded();
        waitForClusterManager();
        clusterManagerClient = ClusterManagerClient
                .create(clusterManagerAddress());
        initCluster();

        startStorageNodeEmbedded();

        client = new StemCluster.Builder()
                .withClusterManagerUrl(clusterManagerAddress())
                .build();

        session = client.newSession();
    }


    @After
    public void tearDown() throws Exception {
        stopStorageNodeEmbedded();
        cleanupDataDirectories();

        stopClusterManagerEmbedded();
        //stopCassandraEmbedded();
        shoutDownZookeeperClients();
        stopZookeeperEmbedded();
        session.close();
    }

    @VisibleForTesting
    protected void shoutDownZookeeperClients() {
        ZookeeperFactoryCached.closeAll();
    }

    private void loadSchema() {
        cassandraTestSession.execute("DROP KEYSPACE IF EXISTS stem");
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("schema.cql");
        if (null == inputStream)
            throw new NullPointerException("Input stream for Schema file can not be null");
        List<String> lines = getLines(inputStream);
        List<String> statements = linesToCQLStatements(lines);

        for (String statement : statements) {
            cassandraTestSession.execute(statement);
        }
    }

    private List<String> linesToCQLStatements(List<String> lines) {
        List<String> statements = new ArrayList<String>();
        StringBuilder statementUnderConstruction = new StringBuilder();
        for (String line : lines) {
            statementUnderConstruction.append(line.trim());
            if (endOfStatementLine(line)) {
                statements.add(statementUnderConstruction.toString());
                statementUnderConstruction.setLength(0);
            } else {
                statementUnderConstruction.append(" ");
            }
        }
        return statements;
    }

    private boolean endOfStatementLine(String line) {
        return line.endsWith(";");
    }

    // TODO: extract CQL statements read logic to separate class
    public List<String> getLines(InputStream inputStream) {
        final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(inputStreamReader);
        String line;
        List<String> cqlQueries = Lists.newArrayList();
        try {
            while ((line = br.readLine()) != null) {
                if (StringUtils.isNotBlank(line)) {
                    cqlQueries.add(line);
                }
            }
            br.close();
            return cqlQueries;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void initCluster() {
        clusterManagerClient.initCluster(getClusterName(), getvBucketsNum(), getRF(), getPartitioner(), true);
    }

    private String getPartitioner() {
        return Partitioner.Type.CRUSH.getName();
    }

    protected String getClusterName() {
        return "Test cluster";
    }

    protected int getvBucketsNum() {
        return 1000;
    }

    protected int getRF() {
        return 1;
    }

    protected void waitForClusterManager() {
        try {
            boolean connected = false;
            int maxCount = 5;
            int count = 0;
            while (!connected && count < maxCount) {
                connected = tryClusterManager();
                if (!connected) {
                    Thread.sleep(500);
                    count++;
                }
            }
            if (count >= maxCount) {
                throw new RuntimeException("Timed out waiting for Blob Manager to start");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Error while waiting for Blob manager instance to be started", e);
        }
    }

    private boolean tryClusterManager() throws InterruptedException {
        try {
            StemResponse info = ClusterManagerClient
                    .create("http://localhost:9997")
                    .info();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    protected void startZookeeperEmbedded() {
        try {
            zookeeperInstance = new TestingServer(2181);
        } catch (Exception e) {
            throw new RuntimeException("Cant start Zookeeper instance", e);
        }
    }


    protected void stopZookeeperEmbedded() {
        try {
            zookeeperInstance.close();
        } catch (IOException e) {
            throw new RuntimeException("Cant stop Zookeeper instance", e);
        }
    }

    protected void startClusterManagerEmbedded() {
        Thread clusterManagerThread = new Thread() {
            @Override
            public void run() {
                daemon = new ClusterManagerDaemon();
                daemon.start();
            }
        };
        clusterManagerThread.start();
    }

    protected void restartClusterManagerEmbedded() {
        stopClusterManagerEmbedded();
        startClusterManagerEmbedded();
    }

    protected void stopClusterManagerEmbedded() {
        daemon.stop();
    }

    private void startCassandraEmbedded() {
        cassandraTestSession = CassandraEmbeddedServerBuilder
                .noEntityPackages()
                .withClusterName("Stem Meta Store")
                .withThriftPort(9160)
                .withCQLPort(9042)
                .cleanDataFilesAtStartup(true)
                .buildNativeSessionOnly();

        //cassandraTestSession.execute("");
    }

    private void stopCassandraEmbedded() {
        cassandraTestSession.close();
    }

    private void cleanupDataDirectories() throws IOException {
        String[] paths = StorageNodeDescriptor.getBlobMountPoints();
        for (String path : paths) {
            TestUtils.emptyDir(path);
        }

        //TestUtil.emptyDir(path);
    }

    private void startStorageNodeEmbedded() {
        StorageNodeDescriptor.loadConfig(); // must be called explicitly
        StorageNodeDaemon.instance.start();
    }

    private void stopStorageNodeEmbedded() {
        StorageNodeDaemon.instance.stop();
    }

    private void setupEnvironment() throws URISyntaxException {
        String yamlPath = getStorageNodeConfigPath();
        System.setProperty("stem.config", yamlPath);
        System.setProperty("stem.node.id", new File(yamlPath).getParentFile().getAbsolutePath() + File.separator + "id");

        // Set Cluster Manager yaml config path
        URL url = YamlConfigurator.convertPathToURL(getClusterManagerConfigName());
        String absolutePath = new File(url.toURI()).getAbsolutePath();
        System.setProperty("stem.cluster.config", absolutePath);
    }

    @AfterClass
    public static void afterClass() throws IOException {
        TestUtils.cleanupTempDir();
    }

    protected static UUID getFirstDiskUUID() {
        return Layout.getInstance().getMountPoints().keySet().iterator().next();
    }

    protected static MountPoint getFirstDisk() {
        return Layout.getInstance().getMountPoints().get(getFirstDiskUUID());
    }

    protected static int getWriteCandidates() {
        return StorageService.instance.getWriteCandidates(getFirstDiskUUID());
    }

    protected WriteBlobMessage getRandomWriteMessage() {
        byte[] blob = TestUtils.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);
        UUID disk = getFirstDiskUUID();

        WriteBlobMessage op = new WriteBlobMessage();
        op.disk = disk;
        op.key = key;
        op.blob = blob;

        return op;
    }

    protected Blob getRandomWriteMessage2() {
        byte[] blob = TestUtils.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);

        return Blob.create(key, blob);
    }

    protected List<byte[]> generateRandomLoad(int blobsNum) {
        List<byte[]> generatedKeys = new ArrayList<byte[]>(blobsNum);
        for (int i = 0; i < blobsNum; i++) {
            byte[] data = TestUtils.generateRandomBlob(65536);
            byte[] key = DigestUtils.md5(data);

            session.put(Blob.create(key, data));
            generatedKeys.add(key);
            System.out.println(String.format("key 0x%s generated", Hex.encodeHexString(key)));
        }

        return generatedKeys;
    }

    protected List<byte[]> generateStaticLoad(int blobsNum) {
        List<byte[]> generatedKeys = new ArrayList<byte[]>(blobsNum);
        for (int i = 0; i < blobsNum; i++) {
            byte[] data = TestUtils.generateZeroBlob(65536);
            data[i] = 1;
            byte[] key = DigestUtils.md5(data);

            session.put(Blob.create(key, data));
            generatedKeys.add(key);
            System.out.println(String.format("key 0x%s generated", Hex.encodeHexString(key)));
        }

        return generatedKeys;
    }

    protected String getStorageNodeConfigPath() {
        String tmpDir = TestUtils.getDirInTmp("storagenode");
        String tmpDataDir = TestUtils.getDirInTmp("storagenode/data");
        YamlConfigurator yamlConfigurator = YamlConfigurator.open(getStorageNodeConfigName());

        yamlConfigurator
                .setBlobMountPoints(tmpDataDir)
                .setFatFileSizeInMb(5)
                .setMaxSpaceAllocationInMb(30);

        customStorageNodeConfiguration(yamlConfigurator);

        return yamlConfigurator
                .saveTo(tmpDir);
    }

    protected void customStorageNodeConfiguration(YamlConfigurator yamlConfigurator) {

    }

    protected String getStorageNodeConfigName() {
        return "stem.yaml";
    }

    protected String getClusterManagerConfigName() {
        return "cluster.yaml";
    }
}
