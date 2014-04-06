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

public class BlobDescriptor
{
    final int FFIndex;
    final int offset;
    final int bodyOffset;

    public BlobDescriptor(int FFIndex, int offset, int bodyOffset)
    {
        this.FFIndex = FFIndex;
        this.offset = offset; // TODO: for FatFiles > 4GB Integer is not enough
        this.bodyOffset = bodyOffset; // TODO: for FatFiles > 4GB Integer is not enough
    }

    public int getFFIndex()
    {
        return FFIndex;
    }

    public int getOffset()
    {
        return offset;
    }

    public int getBodyOffset()
    {
        return bodyOffset;
    }
}
