package com.ensemblu.axiom.reactive.engine;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.foundation.Nothing;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.spec.database.binder.AxiomBinder;
import com.ensemblu.axiom.spec.database.contract.AxiomProtocol;
import io.vertx.sqlclient.Tuple;

import java.util.Date;
import java.util.function.Supplier;

public class ReactiveBinder implements AxiomBinder {
    private final Tuple tuple;

    public ReactiveBinder(Tuple tuple) {
        this.tuple = tuple;
    }

    @Override
    public Result<Nothing> bindString(int i, String v) {
        return bind(() -> tuple.addString(v));
    }//

    @Override
    public Result<Nothing> bindInteger(int i, Integer v) {
        return bind(() -> tuple.addInteger(v));
    }//

    @Override
    public Result<Nothing> bindLong(int i, Long v) {
        return bind(() -> tuple.addLong(v));
    }//

    @Override
    public Result<Nothing> bindDouble(int i, Double v) {
        return bind(() -> tuple.addDouble(v));
    }//

    @Override
    public Result<Nothing> bindBoolean(int i, Boolean v) {
        return bind(() -> tuple.addBoolean(v));
    }//

    @Override
    public Result<Nothing> bindTimestamp(int i, Date v) {
        return bind(() -> tuple.addTemporal(v.toInstant()));
    }//

    @Override
    public Result<Nothing> bindNull(int index, AxiomProtocol protocol) {
        return Axiom.Check.attempt(() -> {
            tuple.addValue(null);
            return Nothing.INSTANCE;
        });
    }

    private Result<Nothing> bind(Supplier<Tuple> action) {
        return Axiom.Check.attempt(action::get).map(_ -> Nothing.INSTANCE);
    }
}