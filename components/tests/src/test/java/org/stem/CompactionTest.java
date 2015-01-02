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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.stem.client.*;
import org.stem.client.old.StemClient;
import org.stem.db.*;
import org.stem.db.Blob;
import org.stem.db.compaction.CompactionManager;
import org.stem.db.compaction.FFScanner;
import org.stem.utils.YamlConfigurator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CompactionTest extends IntegrationTestBase {

    final String host = "localhost";
    final int port = 9999;

    @Ignore
    @Test
    public void testScanner() throws Exception {
        final int BLOBS_NUM = 256 + 1;

        generateRandomLoad(BLOBS_NUM);

        UUID disk = getFirstDiskUUID();
        FatFile fatFile = Layout.getInstance().getMountPoints().get(disk).getFatFile(0);

        FFScanner scanner = new FFScanner(fatFile);

        List<byte[]> retrievedKeys = new ArrayList<byte[]>(BLOBS_NUM);
        while (scanner.hasNext()) {
            Blob blob = scanner.next();
            retrievedKeys.add(blob.getHeader().key);
        }
    }

    @Test
    public void testCompaction() throws Exception {

        DataTracker dt = getFirstDisk().getDataTracker();

        final int BLOBS_NUM = 79 + 79 + 79 + 1; // 5MB fat file may have 79 of 65KB blobs.
        final int BLOB_SIZE = 65536;

        assert dt.getTotalBlobs() == 0;
        assert dt.getLiveBlobs() == 0;
        assert dt.getLiveSizeInBytes() == 0;
        assert dt.getTotalSizeInBytes() == 0;
        assert dt.getDeletesCount() == 0;
        assert dt.getDeletesCount(0) == 0;
        assert dt.getDeletesCount(1) == 0;
        assert dt.getDeletesCount(2) == 0;
        assert dt.getDeletesCount(3) == 0;
        assert dt.getDeletesSizeInBytes(0) == 0;
        assert dt.getDeletesSizeInBytes(1) == 0;
        assert dt.getDeletesSizeInBytes(2) == 0;
        assert dt.getDeletesSizeInBytes(3) == 0;
        assert dt.getDeletesSizeInBytes() == 0;
        assert dt.getBlankFatFileCount() == 20;
        assert dt.getFullFatFileCount() == 0;
        assert dt.getFatFileCount() == 20;
        assert getWriteCandidates() == 19;

        // Perform WRITE
        List<byte[]> keysGenerated = generateRandomLoad(BLOBS_NUM); // put 238 blobs
        byte[] before = session.get(keysGenerated.get(96)).body;

        assert dt.getTotalBlobs() == BLOBS_NUM;
        assert dt.getLiveBlobs() == BLOBS_NUM;
        assert dt.getLiveSizeInBytes() == BLOBS_NUM * BLOB_SIZE;
        assert dt.getTotalSizeInBytes() == BLOBS_NUM * BLOB_SIZE;
        assert dt.getDeletesCount() == 0;
        assert dt.getDeletesCount(0) == 0;
        assert dt.getDeletesCount(1) == 0;
        assert dt.getDeletesCount(2) == 0;
        assert dt.getDeletesCount(3) == 0;
        assert dt.getDeletesSizeInBytes(0) == 0;
        assert dt.getDeletesSizeInBytes(1) == 0;
        assert dt.getDeletesSizeInBytes(2) == 0;
        assert dt.getDeletesSizeInBytes(3) == 0;
        assert dt.getDeletesSizeInBytes() == 0;
        assert dt.getBlankFatFileCount() == 16;
        assert dt.getFullFatFileCount() == 3;
        assert dt.getFatFileCount() == 20;
        assert getWriteCandidates() == 16;

        int deletes = (int) Math.ceil(BLOBS_NUM * StorageNodeDescriptor.getCompactionThreshold() * 4);
        // Delete some data
        for (int i = 0; i < deletes; i++) // Delete 40% of data
        {
            session.delete(keysGenerated.get(i));
        }

        byte[] before2 = session.get(keysGenerated.get(96)).body;

        assert dt.getTotalBlobs() == BLOBS_NUM;
        assert dt.getLiveBlobs() == BLOBS_NUM - deletes;
        assert dt.getLiveSizeInBytes() == (BLOBS_NUM - deletes) * BLOB_SIZE;
        assert dt.getTotalSizeInBytes() == BLOBS_NUM * BLOB_SIZE;
        assert dt.getDeletesCount() == deletes;
        assert dt.getDeletesCount(0) == 79;
        assert dt.getDeletesCount(1) == 96 - 79;
        assert dt.getDeletesCount(2) == 0;
        assert dt.getDeletesCount(3) == 0;
        assert dt.getDeletesSizeInBytes(0) == 79 * BLOB_SIZE;
        assert dt.getDeletesSizeInBytes(1) == (96 - 79) * BLOB_SIZE;
        assert dt.getDeletesSizeInBytes(2) == 0;
        assert dt.getDeletesSizeInBytes(3) == 0;
        assert dt.getDeletesSizeInBytes() == deletes * BLOB_SIZE;
        assert dt.getBlankFatFileCount() == 16;
        assert dt.getFullFatFileCount() == 3;
        assert dt.getFatFileCount() == 20;
        assert getWriteCandidates() == 16;

        // Perform compaction
        CompactionManager.instance.performMajorCompaction();
        byte[] after = session.get(keysGenerated.get(96)).body;

        assert dt.getTotalBlobs() == BLOBS_NUM;
        assert dt.getLiveBlobs() == BLOBS_NUM - deletes;
        assert dt.getLiveSizeInBytes() == (BLOBS_NUM - deletes) * BLOB_SIZE;
        assert dt.getDeletesCount() == 0;
        assert dt.getDeletesCount(0) == 0;
        assert dt.getDeletesCount(1) == 0;
        assert dt.getDeletesCount(2) == 0;
        assert dt.getDeletesCount(3) == 0;
        assert dt.getDeletesSizeInBytes(0) == 0;
        assert dt.getDeletesSizeInBytes(1) == 0;
        assert dt.getDeletesSizeInBytes(2) == 0;
        assert dt.getDeletesSizeInBytes(3) == 0;
        assert dt.getDeletesSizeInBytes() == 0;
        assert dt.getBlankFatFileCount() == 18;
        assert dt.getFullFatFileCount() == 1;
        assert dt.getFatFileCount() == 20;
        assert getWriteCandidates() == 18;


        // Control validation. Read all the dataset we put
        for (int i = 0; i < BLOBS_NUM; i++) {
            byte[] keyOrig = keysGenerated.get(i);

            org.stem.client.Blob blob = session.get(keyOrig);
            if (i < deletes) {
                assert blob == null;
                continue;
            }

            byte[] data = blob.body;
            byte[] keyActual = DigestUtils.md5(data);
            Assert.assertArrayEquals(keyActual, keyOrig);
        }

        // If we here then data have been migrated correctly,we got two new BLANK files for writes
        // and all the data has not been corrupted
    }

    @Override
    protected void customStorageNodeConfiguration(YamlConfigurator yamlConfigurator) {
        yamlConfigurator
                .setFatFileSizeInMb(5)
                .setMaxSpaceAllocationInMb(100)
                .setCompactionThreshold(0.1f);
    }

    @Override
    protected String getStorageNodeConfigName() {
        return "stem.small_ff.yaml";
    }
}
