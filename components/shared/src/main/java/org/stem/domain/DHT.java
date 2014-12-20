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

package org.stem.domain;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.math.BigInteger;

public class DHT {

    private static BigInteger MD5_MAX_VALUE = new BigInteger("2", 10).pow(128);

    private final BigInteger capacity;
    private final int sections;

    public DHT(int sections) {
        this(MD5_MAX_VALUE, sections);
    }

    public DHT(BigInteger capacity, int sections) {

        this.capacity = capacity;
        this.sections = sections;
    }

    public int getSection(String key) {
        try {
            return getSection(Hex.decodeHex(key.toCharArray()));
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    public int getSection(byte[] keyBytes) {
        BigInteger token = new BigInteger(new String(Hex.encodeHex(keyBytes)), 16);
        BigInteger sectionSize = capacity.divide(BigInteger.valueOf(sections));

        int quotient = token.divide(sectionSize).intValue();
        BigInteger mod = token.mod(sectionSize);
        int index;
        if (quotient == 0)
            index = 0;
        else if (mod.equals(BigInteger.ZERO))
            index = Math.max(quotient - 1, 0);
        else index = Math.max(quotient, 0);

        if (index >= sections) {
            return sections - 1;
        }

        return index;
    }
}
