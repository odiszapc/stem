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
import org.junit.Ignore;
import org.junit.Test;
import org.stem.client.MetaStoreClient;
import org.stem.client.Responses;
import org.stem.client.old.StemClient;
import org.stem.client.old.StorageNodeClient;
import org.stem.db.Blob;
import org.stem.db.Layout;
import org.stem.domain.ExtendedBlobDescriptor;
import org.stem.transport.Message;
import org.stem.transport.ops.WriteBlobMessage;
import org.stem.utils.TestUtils;

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

import static org.junit.Assert.assertEquals;

public class ProtocolTest extends IntegrationTestBase {

    final String host = "localhost";
    final int port = 9999;

    @Test
    public void testReadWrite() throws Exception {
        org.stem.client.Blob writeOp = getRandomWriteMessage2();
        final int BLOB_SIZE = writeOp.getBlobSize();

        Responses.Result.WriteBlob resp = session.put(writeOp);
        assertEquals(resp.getFatFileIndex(), 0);
        assertEquals(resp.getOffset(), 1 + Blob.Header.SIZE);

        org.stem.client.Blob readBlob = session.get(writeOp.key);
        Assert.assertArrayEquals(readBlob.body, writeOp.body);

        Responses.Result.WriteBlob resp2 = session.put(writeOp);
        assertEquals(resp2.getFatFileIndex(), 0);
        assertEquals(resp2.getOffset(), resp.getOffset() + Blob.Header.SIZE + BLOB_SIZE);
    }


    @Test
    public void testStorageNodeDeleteThenRead() throws Exception {
        MetaStoreClient meta = new MetaStoreClient("localhost");
        meta.start();

        org.stem.client.Blob writeOp = getRandomWriteMessage2();

        Responses.Result.WriteBlob resp = session.put(writeOp);
        assertEquals(resp.getFatFileIndex(), 0);
        assertEquals(resp.getOffset(), 1 + Blob.Header.SIZE);

        List<ExtendedBlobDescriptor> descriptors = meta.readMeta(writeOp.key);
        assertEquals(1, descriptors.size());
        ExtendedBlobDescriptor d = descriptors.get(0);
        assertEquals(0, d.getFFIndex());
        assertEquals(1 + Blob.Header.SIZE, d.getBodyOffset());
        assertEquals(getFirstMountPoint().getId(), d.getDisk());

        session.delete(writeOp.key);

        descriptors = meta.readMeta(writeOp.key);
        assertEquals(0, descriptors.size());

        meta.stop();
    }


    @Test
    public void testDelete() throws Exception {
        byte[] in = TestUtils.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(in);
        session.put(org.stem.client.Blob.create(key, in));
        org.stem.client.Blob blob = session.get(key);
        Assert.assertArrayEquals(blob.body, in);
    }

    @Test
    @Ignore // TODO: it's ignored because it's ran endlessly
    public void testStorageNodeWritePerformance() throws Exception {
        StorageNodeClient client = new StorageNodeClient(host, port);
        client.start();

        byte[] blob = TestUtils.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);
        UUID disk = Layout.getInstance().getMountPoints().keySet().iterator().next();
        WriteBlobMessage op = new WriteBlobMessage();
        op.disk = disk;
        op.key = key;
        op.blob = blob;

        long start, duration;
        long i = 0;
        start = System.nanoTime();
        while (true) {
            i++;
            Message.Response resp = client.execute(op);
            if (i % 1000 == 0) {
                duration = System.nanoTime() - start;

                long rps = i * 1000000000 / duration;
                System.out.println(String.format("%s req/s, %s/s", rps, FileUtils.byteCountToDisplaySize(rps * blob.length)));
                start = System.nanoTime();
                i = 0;
            }
        }
    }

    @Test
    @Ignore // TODO: it's ignored because it's ran endlessly
    public void testClusterWritePerformance() throws Exception {
        Logger.getLogger("io.netty").setLevel(Level.OFF);

        clusterManagerClient.computeMapping();

        StemClient client = new StemClient();
        client.start();

        byte[] blob = TestUtils.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);

        long start, duration;
        long i = 0;
        start = System.nanoTime();
        while (true) {
            i++;
            client.put(key, blob);
            if (i % 1000 == 0) {
                duration = System.nanoTime() - start;

                long rps = i * 1000000000 / duration;
                System.out.println(String.format("%s req/s, %s/s", rps, FileUtils.byteCountToDisplaySize(rps * blob.length)));
                start = System.nanoTime();
                i = 0;
            }
        }

    }

    @Test
    @Ignore // TODO: it's ignored because it's ran endlessly
    public void testClusterMultiThreadWrite() throws Exception {
        clusterManagerClient.computeMapping();
        MultiSourcesExecutor executor = new MultiSourcesExecutor();
        executor.submitWriters(4);
        executor.shutdown();
    }

    @Test
    @Ignore // TODO: it's ignored because it's ran endlessly
    public void testMultiSourcesWritePerformance() throws Exception {
        StorageNodeClient client = new StorageNodeClient(host, port);
        client.start();

        byte[] blob = TestUtils.generateRandomBlob(65536);
        byte[] key = DigestUtils.md5(blob);
        Set<UUID> disks = Layout.getInstance().getMountPoints().keySet();

        List<WriteBlobMessage> messages = new ArrayList<WriteBlobMessage>(disks.size());
        for (UUID disk : disks) {
            WriteBlobMessage op = new WriteBlobMessage();
            op.disk = disk;
            op.key = key;
            op.blob = blob;
            messages.add(op);
        }

        int threadsNum = messages.size();
        ExecutorService service = Executors.newFixedThreadPool(threadsNum);
        for (int j = 0; j < threadsNum; ++j) {
            ClientThread clientThread = new ClientThread(messages.get(j), j);
            service.submit(clientThread);
        }

        service.shutdown();
        service.awaitTermination(10, TimeUnit.MINUTES);
    }

    public class ClientThread implements Callable<Void> {

        private WriteBlobMessage op;
        private int id;
        private StorageNodeClient client;

        public ClientThread(WriteBlobMessage op, int id) throws IOException {
            this.op = op;
            this.id = id;
            client = new StorageNodeClient(host, port);
            client.start();
        }

        @Override
        public Void call() throws Exception {
            long start, duration;
            long i = 0;
            start = System.nanoTime();
            while (true) {
                i++;

                Message.Response resp = client.execute(op);
                //if (i > 10000) {Thread.sleep(100000000);}
                if (i % 1000 == 0) {
                    duration = System.nanoTime() - start;

                    long rps = i * 1000000000 / duration;
                    System.out.println(String.format("#%s %s req/s, %s/s", id, rps, FileUtils.byteCountToDisplaySize(rps * op.blob.length)));
                    start = System.nanoTime();
                    i = 0;
                }
            }
        }
    }

    private class MultiSourcesExecutor {

        ExecutorService service = Executors.newCachedThreadPool();
        AtomicLong counter = new AtomicLong();
        StemClient client;

        private MultiSourcesExecutor() {
            client = new StemClient();
            client.start();
            service.submit(new Logger());
        }

        public void submitWriters(int num) {
            for (int i = 0; i < num; i++) {
                newWriter();
            }
        }

        public void newWriter() {
            service.submit(new Writer());

        }

        public void shutdown() throws InterruptedException {
            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);
        }

        public class Writer implements Runnable {

            byte[] in = TestUtils.generateRandomBlob(65536);

            @Override
            public void run() {
                StemClient client = new StemClient();
                client.start();

                while (true) {

                    byte[] key = TestUtils.generateRandomBlob(16);

                    try {
                        client.put(key, in);
                        counter.incrementAndGet();
                    } catch (Throwable ex) {
                        int a = 1;
                    }
                }
            }
        }

        public class Logger implements Runnable {

            long tickBefore = System.nanoTime();
            long countBefore = 0;

            @Override
            public void run() {
                while (true) {
                    try {
                        long tickNow = System.nanoTime();
                        long countNow = counter.get();
                        long countDelta = countNow - countBefore;
                        long duration = tickNow - tickBefore;
                        long rps = 0 == duration ? 0 : countDelta * 1000000000 / duration;
                        System.out.println(String.format("%s req/s, %s/s", rps, FileUtils.byteCountToDisplaySize(rps * 65536)));
                        tickBefore = System.nanoTime();
                        countBefore = countNow;

                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
