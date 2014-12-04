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

package org.stem.client;

import com.google.common.util.concurrent.Uninterruptibles;

import java.util.concurrent.ExecutionException;

public class ResponseHandler {
    final DefaultResultFuture future;

    public ResponseHandler(DefaultResultFuture future) {
        this.future = future;
    }

    public Message.Response get() {
        try {
            Message.Response response = Uninterruptibles.getUninterruptibly(future);
            return response;
        } catch (ExecutionException e) {
            e.printStackTrace();
            return null;
        }
    }
}
