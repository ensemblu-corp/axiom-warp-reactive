package com.ensemblu.axiom.reactive.codec;
import io.vertx.core.spi.JsonFactory;
import io.vertx.core.spi.json.JsonCodec;

public class AxiomGhostFactory implements JsonFactory {
    private final JsonCodec codec = new AxiomGhostCodec();
    @Override
    public JsonCodec codec() { return codec; }
}
