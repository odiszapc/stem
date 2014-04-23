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

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.stem.client.StemClient;
import org.stem.client.StorageNodeClient;
import org.stem.db.Blob;
import org.stem.db.Layout;
import org.stem.domain.BlobDescriptor;
import org.stem.transport.Message;
import org.stem.transport.ops.DeleteBlobMessage;
import org.stem.transport.ops.WriteBlobMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ProtocolTest extends IntegrationTestBase
{

    final String host = "localhost";
    final int port = 9999;

    @Test
    public void testConnect() throws Exception
    {

        final String host = "localhost";
        final int port = 9998;

        StorageNodeClient client = new StorageNodeClient(host, port);
        client.start();

//        ReadBlobMessage op = new ReadBlobMessage();
//        op.disk = 1;
//        op.offset = 2;
//        op.length = 3;
//        client.executeSimple(op);
    }

    @Test
    public void testReadWrite() throws Exception
    {
        StorageNodeClient client = new StorageNodeClient(host, port);
        client.start();

        WriteBlobMessage writeOp = getRandomWriteMessage();
        final int BLOB_SIZE = writeOp.getBlobSize();

        // Write
        BlobDescriptor resp = client.writeBlob(writeOp);
        Assert.assertEquals(resp.getFFIndex(), 0);
        Assert.assertEquals(resp.getBodyOffset(), 1 + Blob.Header.SIZE);

        // Read
        byte[] readBlob = client.readBlob(writeOp.disk,
                resp.getFFIndex(),
                resp.getBodyOffset(),
                writeOp.getBlobSize());
        Assert.assertArrayEquals(readBlob, writeOp.blob);

        // Write
        BlobDescriptor resp2 = client.writeBlob(writeOp);
        Assert.assertEquals(resp2.getFFIndex(), 0);
        Assert.assertEquals(resp2.getBodyOffset(), resp.getBodyOffset() + Blob.Header.SIZE + BLOB_SIZE);
    }


    @Test
    public void testStorageNodeDelete() throws Exception
    {
        StorageNodeClient client = new StorageNodeClient(host, port);
        client.start();
        WriteBlobMessage writeOp = getRandomWriteMessage();

        BlobDescriptor resp = client.writeBlob(writeOp);
        Assert.assertEquals(resp.getFFIndex(), 0);
        Assert.assertEquals(resp.getBodyOffset(), 1 + Blob.Header.SIZE);

        DeleteBlobMessage deleteOp = new DeleteBlobMessage(writeOp.disk, resp.getFFIndex(), resp.getBodyOffset());
        client.deleteBlob(deleteOp);
        client.deleteBlob(deleteOp);
    }

    @Test
    public void testDelete() throws Exception
    {
        clusterManagerClient.computeMapping();

        StemClient client = new StemClient();
        client.start();

        byte[] in = TestUtil.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(in);

        client.put(key, in);
        byte[] out = client.get(key);

        // STEM is not a random generator, but storage, so data before and after must be equal
        Assert.assertArrayEquals(out, in);

        client.delete(key);


        client.get(key);
    }

    @Test
    public void testStorageNodeWritePerformance() throws Exception
    {
        StorageNodeClient client = new StorageNodeClient(host, port);
        client.start();

        byte[] blob = TestUtil.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);
        UUID disk = Layout.getInstance().getMountPoints().keySet().iterator().next();
        WriteBlobMessage op = new WriteBlobMessage();
        op.disk = disk;
        op.key = key;
        op.blob = blob;

        long start, duration;
        long i = 0;
        start = System.nanoTime();
        while (true)
        {
            i++;
            Message.Response resp = client.execute(op);
            if (i % 1000 == 0)
            {
                duration = System.nanoTime() - start;

                long rps = i * 1000000000 / duration;
                System.out.println(String.format("%s req/s, %s/s", rps, FileUtils.byteCountToDisplaySize(rps * blob.length)));
                start = System.nanoTime();
                i = 0;
            }
        }
    }

    @Test
    public void testClusterWritePerformance() throws Exception
    {
        Logger.getLogger("io.netty").setLevel(Level.OFF);

        clusterManagerClient.computeMapping();

        StemClient client = new StemClient();
        client.start();

        byte[] blob = TestUtil.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);

        long start, duration;
        long i = 0;
        start = System.nanoTime();
        while (true)
        {
            i++;
            client.put(key, blob);
            if (i % 1000 == 0)
            {
                duration = System.nanoTime() - start;

                long rps = i * 1000000000 / duration;
                System.out.println(String.format("%s req/s, %s/s", rps, FileUtils.byteCountToDisplaySize(rps * blob.length)));
                start = System.nanoTime();
                i = 0;
            }
        }

    }

    @Test
    public void testClusterWrite() throws Exception
    {
        clusterManagerClient.computeMapping();

        StemClient client = new StemClient();
        client.start();

        byte[] in = TestUtil.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(in);

        client.put(key, in);
        byte[] out = client.get(key);

        // STEM is not a random generator, but storage, so data before and after must be equal
        Assert.assertArrayEquals(out, in);

        Thread.sleep(100000000);
    }

    @Test
    public void testClusterMultiThreadWrite() throws Exception
    {
        clusterManagerClient.computeMapping();
        MultiSourcesExecutor executor = new MultiSourcesExecutor();
        executor.submitWriters(4);
        executor.shutdown();

    }

    @Test
    public void testMultiSourcesWritePerformance() throws Exception
    {
        StorageNodeClient client = new StorageNodeClient(host, port);
        client.start();

        byte[] blob = TestUtil.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);
        Set<UUID> disks = Layout.getInstance().getMountPoints().keySet();

        List<WriteBlobMessage> messages = new ArrayList<WriteBlobMessage>(disks.size());
        for (UUID disk : disks)
        {
            WriteBlobMessage op = new WriteBlobMessage();
            op.disk = disk;
            op.key = key;
            op.blob = blob;
            messages.add(op);
        }

        int threadsNum = messages.size();
        ExecutorService service = Executors.newFixedThreadPool(threadsNum);
        for (int j = 0; j < threadsNum; ++j)
        {
            ClientThread clientThread = new ClientThread(messages.get(j), j);
            service.submit(clientThread);
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }

    public class ClientThread implements Callable<Void>
    {

        private WriteBlobMessage op;
        private int id;
        private StorageNodeClient client;

        public ClientThread(WriteBlobMessage op, int id) throws IOException
        {
            this.op = op;
            this.id = id;
            client = new StorageNodeClient(host, port);
            client.start();
        }

        @Override
        public Void call() throws Exception
        {
            long start, duration;
            long i = 0;
            start = System.nanoTime();
            while (true)
            {
                i++;

                Message.Response resp = client.execute(op);
                //if (i > 10000) {Thread.sleep(100000000);}
                if (i % 1000 == 0)
                {
                    duration = System.nanoTime() - start;

                    long rps = i * 1000000000 / duration;
                    System.out.println(String.format("#%s %s req/s, %s/s", id, rps, FileUtils.byteCountToDisplaySize(rps * op.blob.length)));
                    start = System.nanoTime();
                    i = 0;
                }
            }
        }
    }

    private class MultiSourcesExecutor
    {

        ExecutorService service = Executors.newCachedThreadPool();
        AtomicLong counter = new AtomicLong();
        StemClient client;

        private MultiSourcesExecutor()
        {
            client = new StemClient();
            client.start();
            service.submit(new Logger());
        }

        public void submitWriters(int num)
        {
            for (int i = 0; i < num; i++)
            {
                newWriter();
            }
        }

        public void newWriter()
        {
            service.submit(new Writer());

        }

        public void shutdown() throws InterruptedException
        {
            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);
        }

        public class Writer implements Runnable
        {
            byte[] in = TestUtil.generateRandomBlob(65536);

            @Override
            public void run()
            {
                StemClient client = new StemClient();
                client.start();

                while (true)
                {

                    byte[] key = TestUtil.generateRandomBlob(16);

                    try
                    {
                        client.put(key, in);
                        counter.incrementAndGet();
                    }
                    catch (Throwable ex)
                    {
                        int a = 1;
                    }
                }
            }
        }

        public class Logger implements Runnable
        {
            long tickBefore = System.nanoTime();
            long countBefore = 0;

            @Override
            public void run()
            {
                while (true)
                {
                    try
                    {
                        long tickNow = System.nanoTime();
                        long countNow = counter.get();
                        long countDelta = countNow - countBefore;
                        long duration = tickNow - tickBefore;
                        long rps = 0 == duration ? 0 : countDelta * 1000000000 / duration;
                        System.out.println(String.format("%s req/s, %s/s", rps, FileUtils.byteCountToDisplaySize(rps * 65536)));
                        tickBefore = System.nanoTime();
                        countBefore = countNow;

                        Thread.sleep(2000);
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
