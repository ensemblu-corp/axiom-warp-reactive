package com.ensemblu.axiom.reactive.scope;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.reactive.api.ArmableStrike;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Transaction;
import java.util.function.Function;


public interface Async {

    static <T> Future<Result<T>> use(SqlClient conn, Function<SqlClient, Future<Result<T>>> op) {
        return op.apply(conn);
    }

    static <T> Future<Result<T>> transaction(SqlClient client, Function<SqlClient, Future<Result<T>>> businessLogic) {
        SqlConnection conn = (SqlConnection) client;
        return conn.begin()//
                .compose(tx -> businessLogic.apply(conn)//
                        .compose(res -> finalize(tx, res))//
                        .recover(err -> rollback(tx, err))//
                );
    }

    private static <T> Future<Result<T>> finalize(Transaction tx, Result<T> result) {
        return result.isSuccess()
                ? tx.commit().map(_ -> result)
                : tx.rollback().map(_ -> result);
    }

    private static <T> Future<Result<T>> rollback(Transaction tx, Throwable err) {
        return tx.rollback()
                .map(_ -> Axiom.Check.<T>failure("Infrastructure Breach: " + err.getMessage()))
                .otherwise(rollbackErr -> Axiom.Check.failure("Rollback failed: " + rollbackErr.getMessage()));
    }


    static Future<Result<PersistentList<PersistentMap<String, Object>>>> useAsGateway(
            SqlClient conn, ArmableStrike strike) {
        return Async.use(conn, strike::arm);
    }

    static Future<Result<PersistentList<PersistentMap<String, Object>>>> transactionAsGateway(
            SqlClient conn, ArmableStrike strike) {
        return Async.transaction(conn, strike::arm);
    }
}