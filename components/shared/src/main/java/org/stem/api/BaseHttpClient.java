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

package org.stem.api;

import org.apache.http.HttpHeaders;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.AbstractHttpMessage;
import org.stem.api.request.ClusterManagerRequest;
import org.stem.api.response.StemResponse;
import org.stem.utils.JsonUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;


public abstract class BaseHttpClient {

    protected URI root;
    protected CloseableHttpClient client;


    public BaseHttpClient(String uri) {
        try {
            this.root = new URI(uri);
            client = HttpClients.createDefault();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    protected static void setHeaders(AbstractHttpMessage request) {
        request.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
        request.addHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.toString());
    }

    protected static StringEntity prepareJsonBody(Object obj) {
        try {
            return new StringEntity(JsonUtils.encode(obj));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public <T extends StemResponse> T send(HttpEntityEnclosingRequestBase request, ClusterManagerRequest msg, Class<T> clazz) {
        try {
            setHeaders(request);
            StringEntity body = prepareJsonBody(msg);

            request.setEntity(body);
            return (T) client.execute(request, getResponseHandler(clazz));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public <T extends StemResponse> T send(HttpRequestBase request, Class<T> clazz) {
        try {
            setHeaders(request);
            return (T) client.execute(request, getResponseHandler(clazz));

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public <T extends StemResponse> T sendGet(String uriStr, Class<T> clazz) {
        try {
            URI uri = getURI(uriStr);
            HttpGet request = new HttpGet(uri);

            setHeaders(request);
            return (T) client.execute(request, getResponseHandler(clazz));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    protected CloseableHttpClient getClient() {
        return client;
    }

    protected URI getURI(String relativeUri) {
        try {
            return new URIBuilder(root)
                    .setPath(relativeUri)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected ResponseHandler getResponseHandler(Class<? extends StemResponse> clazz) {
        return new JsonResponseHandler(clazz);
    }
}
