package com.ensemblu.axiom.reactive.codec;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.spi.json.JsonCodec;

public final class AxiomGhostCodec implements JsonCodec {

    @Override
    public <T> T fromString(String json, Class<T> clazz) throws DecodeException {
        return null;
    }

    @Override
    public <T> T fromBuffer(Buffer json, Class<T> clazz) throws DecodeException {
        return null;
    }

    @Override
    public <T> T fromValue(Object json, Class<T> toValueType) {
        return null;
    }

    @Override
    public String toString(Object object, boolean pretty) {
        return object == null ? null : object.toString();
    }

    @Override
    public Buffer toBuffer(Object object, boolean pretty) {
        if (object == null) return null;
        if (object instanceof Buffer) return (Buffer) object;
        return Buffer.buffer(object.toString());
    }
}