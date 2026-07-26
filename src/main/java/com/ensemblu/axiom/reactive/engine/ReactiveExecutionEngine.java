package com.ensemblu.axiom.reactive.engine;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.reactive.engine.core.ExecutionEngine;
import com.ensemblu.axiom.reactive.syntax.BareVertx;
import com.ensemblu.axiom.spec.database.binder.IngressBinder; // From spec
import com.ensemblu.axiom.spec.database.contract.StrikeInstruction;
import com.ensemblu.axiom.spec.parser.SqlParser;
import io.vertx.core.Future;
import io.vertx.sqlclient.*;

public class ReactiveExecutionEngine implements ExecutionEngine<PersistentList<PersistentMap<String, Object>>> {

    private final SqlClient client;

    public ReactiveExecutionEngine(SqlClient client) {
        this.client = client;
    }

    @Override
    public Future<Result<PersistentList<PersistentMap<String, Object>>>> bindAndExecute(//
            SqlParser.ExecutionPlan plan,//
            StrikeInstruction instr,//
            String translatedSql) { //

        final var tuple = Tuple.tuple();
        IngressBinder.apply(new ReactiveBinder(tuple), plan, instr.types(), instr.data());

        return client.preparedQuery(translatedSql) //
                .execute(tuple)//
                .map(rows -> {//
                    if (rows.columnsNames() == null || rows.columnsNames().isEmpty()) {//
                        final var originalData = instr.data();//
                        final var map = Axiom.Data//
                                .<String, Object>emptyMap()//
                                .put("count", rows.rowCount())//
                                .put("snapshot", originalData); //

                        return Axiom.Check.success(Axiom.Data.<PersistentMap<String, Object>>emptyList().append(map));
                    } else {
                        return Axiom.Check.success(ReactiveResultConverter.convert(rows));
                    }
                })//
                .recover(err -> BareVertx.fastSuccess(Axiom.Check.failure(new RuntimeException("AXIOM BREACH | Bulk execution failed", err))));
    }
}