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

package org.stem.transport.ops;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.stem.exceptions.ErrorCode;
import org.stem.exceptions.ServerError;
import org.stem.exceptions.TransportException;
import org.stem.transport.Message;
import org.stem.util.BBUtils;

public class ErrorMessage extends Message.Response {

    public static final Message.Codec<ErrorMessage> codec = new Message.Codec<ErrorMessage>() {
        @Override
        public ByteBuf encode(ErrorMessage message) {
            ByteBuf codeBuf = BBUtils.intToBB(message.error.code().value);
            ByteBuf msgBuf = BBUtils.stringToBB(message.error.getMessage());

            return Unpooled.wrappedBuffer(codeBuf, msgBuf);
        }

        @Override
        public ErrorMessage decode(ByteBuf buf) {
            ErrorCode code = ErrorCode.fromValue(buf.readInt());
            String message = BBUtils.readString(buf);

            TransportException e = null;

            switch (code) {
                case SERVER_ERROR:
                    e = new ServerError(message);
                    break;
            }


            return new ErrorMessage(e);
        }
    };

    public final TransportException error;

    public TransportException getError() {
        return error;
    }

    public ErrorMessage(TransportException error) {
        super(Type.ERROR);
        this.error = error;
    }


    @Override
    public ByteBuf encode() {
        return codec.encode(this);
    }

    public static ErrorMessage fromException(Throwable e) {
        return new ErrorMessage(new ServerError(e));
    }
}
