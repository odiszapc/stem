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
import org.junit.Test;
import org.stem.db.DataTracker;
import org.stem.db.FatFile;
import org.stem.db.FatFileAllocator;

public class BlobWriteTest extends IntegrationTestBase
{
    @Test
    public void testWriteSingleBlob() throws Exception
    {
        String path = TestUtil.temporize("stem-ff-000001.db");
        FatFileAllocator.allocateFile(path, 4);

        FatFile fatFile = FatFile.open(path, new DataTracker(10));
        fatFile.writeActiveMarker();

        while (fatFile.hasSpaceFor(65536))
        {
            byte[] blob = createBlob(65536);
            String key = DigestUtils.md5Hex(blob);

            fatFile.writeBlob(key, blob);
        }
        fatFile.writeIndex();
        fatFile.writeFullMarker();
    }

    private byte[] createBlob(int size)
    {
        byte[] blob = new byte[size];
        for (int i = 0; i < size; i++)
        {
            blob[i] = 1;
        }
        return blob;
    }
}
