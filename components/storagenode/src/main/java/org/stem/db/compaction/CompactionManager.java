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

package org.stem.db.compaction;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stem.client.MetaStoreClient;
import org.stem.db.*;
import org.stem.domain.BlobDescriptor;
import org.stem.domain.ExtendedBlobDescriptor;
import org.stem.transport.ops.WriteBlobMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

public class CompactionManager {
    private static final Logger logger = LoggerFactory.getLogger(CompactionManager.class);

    public static final CompactionManager instance;
    public static final String TMP_DIR = System.getProperty("java.io.tmpdir"); // TODO: move to StorageNodeDescriptor
    private float threshold = StorageNodeDescriptor.getCompactionThreshold();
    private static MetaStoreClient client;

    static {
        instance = new CompactionManager();
    }

    public CompactionManager() {
        client = new MetaStoreClient();
        client.start(); // TODO: close ?
    }

    public void performMajorCompaction() {
        try {
            Collection<MountPoint> mountPoints = Layout.getInstance().getMountPoints().values();
            for (MountPoint mp : mountPoints) {
                performSinglePassCompaction(mp); // TODO: every disk should be compacted in a separate thread
            }
        } catch (Exception e) {
            throw new RuntimeException("Compaction was stopped unexpectedly", e);
        }
    }

    // TODO: update DataTracker
    private void performSinglePassCompaction(MountPoint mp) throws IOException {
        if (!exceedThreshold(mp))
            return;

        // Get FULL files ready for compaction
        Collection<FatFile> scanReadyFFs = mp.findReadyForCompaction();

        if (!exceedCandidatesThreshold(scanReadyFFs))
            return;

        Queue<FatFile> originalFFs = new LinkedList<FatFile>();

        FatFile temporaryFF = null;
        int iterated = 0;
        int omitted = 0;
        for (FatFile currentFF : scanReadyFFs) {
            FFScanner scanner = new FFScanner(currentFF);

            while (scanner.hasNext()) {
                iterated += 1;
                Blob blob = scanner.next();
                String blobKey = Hex.encodeHexString(blob.key());
                if (blob.deleted()) {
                    omitted += 1;
                    mp.getDataTracker().removeDeletes(blob.key(), blob.size(), currentFF.id);
                    logger.info("key 0x{} omitted as deleted", Hex.encodeHexString(blob.key()));
                    continue;
                }

                ExtendedBlobDescriptor localDescriptor = new ExtendedBlobDescriptor(blob.key(), blob.size(), mp.uuid, blob.getDescriptor());
                ExtendedBlobDescriptor remoteDescriptor = client.readMeta(blob.key(), mp.uuid);
                if (null == remoteDescriptor) {
                    omitted += 1;
                    logger.info("key 0x{} omitted as no meta info", Hex.encodeHexString(blob.key()));
                    continue;
                }
                // As we eventual consistent then: if blob.hasInvalidOffset -> continue
                if (!descriptorsAreConsistent(localDescriptor, remoteDescriptor)) {
                    logger.info("key 0x{} omitted as inconsistent meta", Hex.encodeHexString(blob.key()));
                    continue;
                }

                if (null == temporaryFF) {
                    temporaryFF = createTemporaryFF(currentFF.id);
                }

                if (temporaryFF.hasSpaceFor(blob)) {
                    BlobDescriptor descriptor = temporaryFF.writeBlob(blob); // TODO: hold descriptors for a subsequent MetaStore updates
                    logger.info("key 0x{} is written to temporaryFF", Hex.encodeHexString(blob.key()));
                    continue;
                }

                // If we are here then we can't write blob to temporary file because the temporaryFF is full

                // mark temporaryFF FULL
                temporaryFF.writeIndex();
                temporaryFF.writeFullMarker();

                // Replace original FF with temporary FF
                FatFile originalFF = originalFFs.poll();
                replaceFF(originalFF, temporaryFF);
                updateMeta(originalFF, mp);
                markAllOriginalFFsAsBlank(originalFFs, mp);

                temporaryFF.close(); // TODO: this must be strictly synchronized
                FileUtils.forceDelete(new File(temporaryFF.getPath())); // remove file
                temporaryFF = null;

                // Once temporary file exceeded its capacity create another one
                temporaryFF = createTemporaryFF(currentFF.id);
                // And write blob to it
                BlobDescriptor descriptor = temporaryFF.writeBlob(blob); // TODO: hold descriptors for a subsequent MetaStore updates
            }

            originalFFs.add(currentFF); // When compaction finish this file would be marked as BLANK
        }

        // All candidates are iterated
        // Write the rest of TMP FatFile to StorageNode as usual and mark iterated FFs as BLANK
        if (null != temporaryFF) {
            FFScanner scanner = new FFScanner(temporaryFF);
            int restBlobs = 0;
            while (scanner.hasNext()) {

                restBlobs += 1;
                Blob blob = scanner.next();
                WriteBlobMessage message = new WriteBlobMessage(mp.uuid, blob.key(), blob.data());// TODO: direct access to fields?
                mp.getDataTracker().remove(blob.key(), blob.size());
                BlobDescriptor descriptor = StorageService.instance.write(message);

                // TODO: too heterogeneous. Should be Blob.Descriptor or something like that
                ExtendedBlobDescriptor extDescriptor = new ExtendedBlobDescriptor(blob.key(), blob.size(), mp.uuid, descriptor);
                logger.info("key 0x{} moved", Hex.encodeHexString(blob.key()));

                client.updateMeta(extDescriptor);
            }
            temporaryFF.close();
            FileUtils.forceDelete(new File(temporaryFF.getPath())); // remove file
            temporaryFF = null;
        }

        // Mark the rest of files as BLANK
        markAllOriginalFFsAsBlank(originalFFs, mp);
        if (null != temporaryFF) {
            FileUtils.forceDelete(new File(temporaryFF.getPath())); // remove file
            temporaryFF = null;
        }


        // TODO: delete temporary file
    }

    private static boolean descriptorsAreConsistent(ExtendedBlobDescriptor local, ExtendedBlobDescriptor remote) {
        return local.getDisk().equals(remote.getDisk()) &
                local.getBodyOffset() == remote.getBodyOffset() &
                local.getLength() == remote.getLength();
    }

    private static void markAllOriginalFFsAsBlank(Queue<FatFile> originalFFs, MountPoint mp) throws IOException {
        while (!originalFFs.isEmpty()) {
            FatFile ff = originalFFs.poll();
            ff.reallocate();
            StorageService.instance.submitFF(ff, mp); // TODO: inside we must re-count DataTracker
        }
    }

    private static boolean exceedThreshold(MountPoint mp) {
        long deletesSizeInBytes = mp.getDataTracker().getDeletesSizeInBytes();
        long totalSizeInBytes = mp.getDataTracker().getTotalSizeInBytes();

        float ratio = (float) deletesSizeInBytes / totalSizeInBytes; // TODO: zero check

        if (ratio < StorageNodeDescriptor.getCompactionThreshold())
            return false;

        return true;
    }

    private boolean exceedCandidatesThreshold(Collection<FatFile> candidates) {
        if (candidates.isEmpty())
            return false;

        if (candidates.size() < 2)
            return false; // Yes, we have files ready for compaction, with a single file we have nothing to do

        return true;
    }

    private static void replaceFF(FatFile original, FatFile replacement) throws IOException {
        assert original.size() == replacement.size();
        FileOutputStream out = new FileOutputStream(original.getPath());
        FileInputStream in = new FileInputStream(replacement.getPath());
        IOUtils.copy(in, out);
        in.close();
        out.close();
    }

    // TODO: mount point should be extracted from FatFile instance because the last one is attached to the first one
    private static void updateMeta(FatFile original, MountPoint mp) {
        // Update meta
        FFExtendedScanner scanner = new FFExtendedScanner(original);
        while (scanner.hasNext()) {
            ExtendedBlobDescriptor d = scanner.next();
            d.setDisk(mp.uuid);

            client.updateMeta(d.getKey(), d.getDisk(), original.id, d.getBodyOffset(), d.getLength());
        }
    }

    private static FatFile createTemporaryFF(int id) throws IOException {
        String path = TMP_DIR + File.separator + FatFileAllocator.buildFileName(id);
        File file = new File(path);
        FileUtils.deleteQuietly(file);
        FatFile ff = FatFileAllocator.create(
                path,
                StorageNodeDescriptor.getFatFileSizeInMb());// TODO: is file exists

        //if (0 == ff.getPointer())  // This line and the same line in WriteController must be encapsulated in FatFile class
        ff.markActive();
        return ff;
    }
}
