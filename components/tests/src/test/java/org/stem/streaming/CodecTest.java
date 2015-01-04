/*
 * Copyright 2015 Alexey Plotnik
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

package org.stem.streaming;

import org.junit.Assert;
import org.junit.Test;
import org.stem.api.REST;
import org.stem.coordination.Codecs;

import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class CodecTest {

    @Test
    public void packUnpack() throws Exception {
        REST.StreamingSession original = new REST.StreamingSession(UUID.randomUUID(), preparePartitions(0, 100000-1));
        byte[] packed = original.encode();
        Assert.assertTrue(packed.length < 600 * 1024 * 1024); // 600KB to fit znode

        REST.StreamingSession decoded = Codecs.JSON.decode(packed, REST.StreamingSession.class);
        assertEquals(original.getId(), decoded.getId());
        assertArrayEquals(original.getPartitions(), decoded.getPartitions());
    }

    private Long[] preparePartitions(int from, int to) {
        Long[] longs = new Long[to - from + 1];
        for (int i = 0; i <= to - from; i++)
            longs[i] = (long) i + from;
        return longs;
    }
}
