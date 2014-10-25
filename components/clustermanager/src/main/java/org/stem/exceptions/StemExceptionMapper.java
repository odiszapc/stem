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

import org.stem.api.response.ErrorResponse;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
// TODO: handle any other exceptions
public class StemExceptionMapper implements ExceptionMapper<StemException> {
    @Override
    public Response toResponse(StemException e) {
        ErrorResponse resp = new ErrorResponse();
        resp.setError(String.format("Application error: %s", e.getMessage()));
        resp.setErrorCode(100);
        return Response.status(Response.Status.FORBIDDEN).
                entity(resp).build();
    }
}
