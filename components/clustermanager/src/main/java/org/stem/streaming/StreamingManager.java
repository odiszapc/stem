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

package org.stem.streaming;

public class StreamingManager {

    public static final StreamingManager instance = new StreamingManager();

    private volatile boolean isInit;

    public StreamingManager() {
    }

    public synchronized void init() {
        if (isInit)
            return;
        // DO job;

        isInit = true;
    }

    public void submit(StreamingSession session) {

    }

    void loadSessions() {
    }
}
