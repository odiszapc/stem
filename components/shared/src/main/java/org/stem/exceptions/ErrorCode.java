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

package org.stem.exceptions;

import java.util.HashMap;
import java.util.Map;

public enum ErrorCode
{
    SERVER_ERROR(0x0000),

    TIMEOUT(0x1000),
    UNAVAILABLE(0x1001);

    public int value;
    private static final Map<Integer, ErrorCode> map = new HashMap<Integer, ErrorCode>(ErrorCode.values().length);

    static
    {
        for (ErrorCode code : ErrorCode.values())
            map.put(code.value, code);
    }

    ErrorCode(int value)
    {
        this.value = value;
    }

    public static ErrorCode fromValue(int value)
    {
        ErrorCode code = map.get(value);
        if (code == null)
            throw new IllegalArgumentException(String.format("Unknown error code %d", value));
        return code;
    }
}
