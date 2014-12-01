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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.binary.StringUtils;
import org.junit.Test;
import org.stem.client.Blob;
import org.stem.client.Session;
import org.stem.client.StemCluster;


public class NewClientTest //extends IntegrationTestBase
{
    @Test
    public void testConnect() throws Exception {
        StemCluster cluster = new StemCluster.Builder()
                .withClusterManagerUrl("http://127.0.0.1:9997")
                .build();

        Session session = cluster.connect();

        session.put(Blob.create(Hex.decodeHex("01010101010101010101010101010101".toCharArray()), "binary data".getBytes()));

        Thread.sleep(1000000);
    }
}
