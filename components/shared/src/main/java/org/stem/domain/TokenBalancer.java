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
import java.util.List;

public class TokenBalancer<T>
{

    private static BigInteger MD5_MAX_VALUE = new BigInteger("2", 10).pow(128);

    protected List<T> keySet;

    public TokenBalancer()
    {
    }

    public TokenBalancer(List<T> keySet)
    {
        this.keySet = keySet;
    }

    public int size()
    {
        return keySet.size();
    }

    public T getToken(String key)
    {
        try
        {
            return getToken(Hex.decodeHex(key.toCharArray()));
        }
        catch (DecoderException e)
        {
            throw new RuntimeException(e);
        }
    }

    public T getToken(byte[] keyBytes)
    {
        BigInteger token = new BigInteger(new String(Hex.encodeHex(keyBytes)), 16);


        BigInteger delta = MD5_MAX_VALUE.divide(BigInteger.valueOf(keySet.size()));
        int keyIndex = Math.max(token.divide(delta).intValue() - 1, 0);

        if (keyIndex >= keySet.size())
        {
            throw new RuntimeException("Token " + keyBytes + " is out of range");
        }

        return keySet.get(keyIndex);
    }
}