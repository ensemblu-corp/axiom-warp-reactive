package com.ensemblu.axiom.reactive.engine;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.api.TargetNavigator;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.foundation.DataCast;
import com.ensemblu.axiom.core.foundation.Dop;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.spec.database.materializer.ResultRow;
import io.vertx.sqlclient.Row;

public record ReactiveResultRow(Row row) implements ResultRow {

    private Object getValue(String column) {
        return row.getValue(column);
    }

    @Override
    public TargetNavigator navigate(String column) {
        return new TargetNavigator() {
            @Override
            public <T> Result<T> execute(DataCast.Protocol protocol) {
                final var val = getValue(column);
                if (val == null) return Axiom.Check.failure("Column not found: " + column);
                return DataCast.cast(Dop.normalize(val), protocol);
            }
        };
    }

    @Override
    public PersistentList<String> columns() {
        return Dop.<String>projectList()//
                .pour(0) //
                .whileTrue(i -> i < row.size())//
                .extract(row::getColumnName)//
                .deploy();
    }
}