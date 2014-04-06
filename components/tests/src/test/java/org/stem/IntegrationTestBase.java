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
import com.google.common.collect.Lists;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.stem.api.BlobManagerClient;
import org.stem.api.response.StemResponse;
import org.stem.client.StemClient;
import org.stem.db.Layout;
import org.stem.db.MountPoint;
import org.stem.db.StorageNodeDescriptor;
import org.stem.db.StorageService;
import org.stem.service.StemDaemon;
import org.stem.transport.ops.WriteBlobMessage;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class IntegrationTestBase
{
    BlobManagerLauncher blobManagerInstance;
    private TestingServer zookeeperInstance;
    protected BlobManagerClient blobManagerClient;
    protected Session cassandraTestSession;
    protected StemClient client = new StemClient();

    @Before
    public void setUp() throws IOException
    {
        TestUtil.cleanupTempDir();
        TestUtil.createTempDir();

        setupEnvironment();
        cleanupDataDirectories();

        startZookeeperEmbedded();
        startCassandraEmbedded();
        loadSchema();
        startBlobManagerEmbedded();
        waitForBlobManager();
        blobManagerClient = BlobManagerClient
                .create("http://localhost:9997");
        initCluster();
        startStorageNodeEmbedded();
    }


    @After
    public void tearDown() throws Exception
    {
//  TODO: turn off tear down for a while
//        stopStorageNodeEmbedded();
//        cleanupDataDirectories();
//        stopBlobManagerEmbedded();
//        stopCassandraEmbedded();
//        stopZookeeperEmbedded();
    }

    private void loadSchema()
    {
        try
        {
            File file = new File("docs/schema.cql");
            FileInputStream inputStream = new FileInputStream(file);
            List<String> lines = getLines(inputStream);
            List<String> statements = linesToCQLStatements(lines);

            for (String statement : statements)
            {
                cassandraTestSession.execute(statement);
            }

            //Thread.sleep(10000000);
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    private List<String> linesToCQLStatements(List<String> lines)
    {
        List<String> statements = new ArrayList<String>();
        StringBuilder statementUnderConstruction = new StringBuilder();
        for (String line : lines)
        {
            statementUnderConstruction.append(line.trim());
            if (endOfStatementLine(line))
            {
                statements.add(statementUnderConstruction.toString());
                statementUnderConstruction.setLength(0);
            } else
            {
                statementUnderConstruction.append(" ");
            }
        }
        return statements;
    }

    private boolean endOfStatementLine(String line)
    {
        return line.endsWith(";");
    }

    // TODO: extract CQL statements read logic to separate class
    public List<String> getLines(InputStream inputStream)
    {
        final InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader br = new BufferedReader(inputStreamReader);
        String line;
        List<String> cqlQueries = Lists.newArrayList();
        try
        {
            while ((line = br.readLine()) != null)
            {
                if (StringUtils.isNotBlank(line))
                {
                    cqlQueries.add(line);
                }
            }
            br.close();
            return cqlQueries;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void initCluster()
    {
        blobManagerClient.initCluster("Test cluster", getVBuckets(), getRF());
    }

    protected int getVBuckets()
    {
        return 1000;
    }

    protected int getRF()
    {
        return 1;
    }

    private void waitForBlobManager()
    {
        try
        {
            boolean connected = false;
            int maxCount = 5;
            int count = 0;
            while (!connected && count < maxCount)
            {
                connected = tryBlobManager();
                if (!connected)
                {
                    Thread.sleep(500);
                    count++;
                }
            }
            if (count >= maxCount)
            {
                throw new RuntimeException("Timed out waiting for Blob Manager to start");
            }
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException("Error while waiting for Blob manager instance to be started", e);
        }
    }

    private boolean tryBlobManager() throws InterruptedException
    {
        try
        {
            StemResponse info = BlobManagerClient
                    .create("http://localhost:9997")
                    .info();
        }
        catch (Exception e)
        {
            return false;
        }
        return true;
    }

    private void startZookeeperEmbedded()
    {
        try
        {
            zookeeperInstance = new TestingServer(2181);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Cant start Zookeeper instance", e);
        }
    }


    private void stopZookeeperEmbedded()
    {
        try
        {
            zookeeperInstance.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Cant stop Zookeeper instance", e);
        }
    }

    private void startBlobManagerEmbedded()
    {
        Thread blobManagerThread = new Thread()
        {
            @Override
            public void run()
            {
                blobManagerInstance = new BlobManagerLauncher();
                blobManagerInstance.start();
            }
        };
        blobManagerThread.start();
    }

    private void stopBlobManagerEmbedded()
    {
        blobManagerInstance.stop();
    }

    private void startCassandraEmbedded()
    {
        cassandraTestSession = CassandraEmbeddedServerBuilder
                .noEntityPackages()
                .withClusterName("Stem Meta Store")
                .withThriftPort(9160)
                .withCQLPort(9042)
                .cleanDataFilesAtStartup(true)
                .buildNativeSessionOnly();

        //cassandraTestSession.execute("");
    }

    private void stopCassandraEmbedded()
    {
        cassandraTestSession.close();
    }

    private void stopStorageNodeEmbedded()
    {
        StemDaemon.instance.stop();
    }

    private void cleanupDataDirectories() throws IOException
    {
        String[] paths = StorageNodeDescriptor.getBlobMountPoints();
        for (String path : paths)
        {
            TestUtil.emptyDir(path);
        }

        //TestUtil.emptyDir(path);
    }

    private void startStorageNodeEmbedded()
    {
        StemDaemon.instance.start();
    }

    private String setupEnvironment()
    {
        return System.setProperty("stem.config", getStorageNodeConfigPath());
    }

    @AfterClass
    public static void afterClass() throws IOException
    {
        TestUtil.cleanupTempDir();
    }

    protected static UUID getFirstDiskUUID()
    {
        return Layout.getInstance().getMountPoints().keySet().iterator().next();
    }

    protected static MountPoint getFirstDisk()
    {
        return Layout.getInstance().getMountPoints().get(getFirstDiskUUID());
    }

    protected static int getWriteCandidates()
    {
        return StorageService.instance.getWriteCandidates(getFirstDiskUUID());
    }

    protected WriteBlobMessage getRandomWriteMessage()
    {
        byte[] blob = TestUtil.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);
        UUID disk = getFirstDiskUUID();

        WriteBlobMessage op = new WriteBlobMessage();
        op.disk = disk;
        op.key = key;
        op.blob = blob;

        return op;
    }

    protected List<byte[]> generateRandomLoad(int blobsNum)
    {
        List<byte[]> generatedKeys = new ArrayList<byte[]>(blobsNum);
        for (int i = 0; i < blobsNum; i++)
        {
            byte[] data = TestUtil.generateRandomBlob(65536);
            byte[] key = DigestUtils.md5(data);

            client.put(key, data);
            generatedKeys.add(key);
            System.out.println(String.format("key 0x%s generated", Hex.encodeHexString(key)));
        }

        return generatedKeys;
    }

    protected List<byte[]> generateStaticLoad(int blobsNum)
    {
        List<byte[]> generatedKeys = new ArrayList<byte[]>(blobsNum);
        for (int i = 0; i < blobsNum; i++)
        {
            byte[] data = TestUtil.generateZeroBlob(65536);
            data[i] = 1;
            byte[] key = DigestUtils.md5(data);

            client.put(key, data);
            generatedKeys.add(key);
            System.out.println(String.format("key 0x%s generated", Hex.encodeHexString(key)));
        }

        return generatedKeys;
    }

    protected String getStorageNodeConfigPath()
    {
        return "../stem-storagenode/src/test/resources/stem.yaml";
    }
}
