package com.ensemblu.axiom.reactive.ingest;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.reactive.engine.dialect.Dialect;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;

import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public interface Pipeline {

    int BATCH_SIZE = 1_000;

    static AddPath withDialect(final Dialect dialect) {
        return path -> headers -> new DefaultPipeline(path, headers, dialect);
    }

    @FunctionalInterface interface WithDialect { AddPath withDialect(final Dialect dialect); }

    @FunctionalInterface interface AddPath { AddPipeline fromFilePath(final String path); }

    @FunctionalInterface
    interface AddPipeline {
        default Pipeline basedOnHeaders(String... headers) {
            return basedOnHeaders(Axiom.Data.list(headers));
        }

        default Pipeline usingFileHeaders() {
            return basedOnHeaders(Axiom.Data.emptyList());
        }

        Pipeline basedOnHeaders(PersistentList<String> headers);
    }

    Pipeline map(UnaryOperator<PersistentMap<String, Object>> transform);
    Pipeline filter(Predicate<PersistentMap<String, Object>> predicate);
    ArmableFile onTableName(String tableName);

    @FunctionalInterface interface ArmableFile { Future<Result<Long>> arm(SqlClient client); }

}