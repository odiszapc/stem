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

package org.stem.http.api;

import org.glassfish.grizzly.http.server.HttpServer;

import java.io.IOException;

/**
 * Created by Lucas Amorim on 2014-08-23.
 */
public class PublicApi {

    private HttpServer server;

    public static void main(String args[]) {

        try {
            PublicApi api = new PublicApi();
            api.start();
        }
        catch (Exception e) {
            System.err.println(e);
        }
    }

    public void start() {
        server = new HttpServer();
        try {
            server.start();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        if (server != null) {
            server.shutdownNow();
        }
    }
}
