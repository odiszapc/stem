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

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.stem.api.response.ErrorResponse;
import org.stem.api.response.StemResponse;
import org.stem.util.JsonUtils;

import java.io.IOException;
import java.io.InputStream;

public class JsonResponseHandler implements ResponseHandler<StemResponse> {
    private Class<? extends StemResponse> clazz;

    public JsonResponseHandler(Class<? extends StemResponse> clazz) {
        this.clazz = clazz;
    }

    @Override
    public StemResponse handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
        StatusLine statusLine = response.getStatusLine();
        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();

        if (statusLine.getStatusCode() >= 300) {
            try {
                ErrorResponse errorResponse = JsonUtils.decode(content, ErrorResponse.class);

                String message = String.format("%s [%s]: Code: %s, Message: %s", statusLine.getReasonPhrase(),
                        statusLine.getStatusCode(),
                        errorResponse.getErrorCode(),
                        errorResponse.getError());

                throw new HttpResponseException(
                        statusLine.getStatusCode(),
                        message);
            }
            catch (RuntimeException e) {
                throw new HttpResponseException(
                        statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
            }
        }

        if (entity == null) {
            throw new ClientProtocolException("Response contains no content");
        }

        //SimpleType simpleType = SimpleType.construct(clazz);
        return JsonUtils.decode(content, clazz);
    }
}
