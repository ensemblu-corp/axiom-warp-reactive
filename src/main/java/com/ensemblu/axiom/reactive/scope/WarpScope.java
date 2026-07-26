package com.ensemblu.axiom.reactive.scope;

import com.ensemblu.axiom.api.Axiom;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.reactive.api.ArmableStrike;
import com.ensemblu.axiom.reactive.engine.ReactiveExecutionEngine;
import com.ensemblu.axiom.reactive.engine.core.SovereignGate;
import com.ensemblu.axiom.reactive.engine.dialect.Dialect;
import com.ensemblu.axiom.spec.database.contract.StrikeInstruction;
import com.ensemblu.axiom.spec.parser.SqlParser;
import io.vertx.core.Future;
import com.ensemblu.axiom.reactive.syntax.BareVertx;
import io.vertx.sqlclient.SqlClient;


public interface WarpScope {

    static Future<Result<PersistentList<PersistentMap<String, Object>>>> forkAndJoin(
            SqlClient conn, //
            PersistentList<StrikeInstruction> instructions,//
            ParallelGateway gateway,//
            Dialect dialect) {//

        final var tasks = instructions.map(instr -> {
            final var plan = SqlParser.forge(instr.sql());
            final var translatedSql = dialect.translate(plan.sql());
            ArmableStrike strike = c -> SovereignGate.execute(instr, plan, translatedSql, new ReactiveExecutionEngine(c));

            return gateway.apply(conn, strike);
        });

        final Future<Result<PersistentList<PersistentMap<String, Object>>>>[] futureArray = new Future[tasks.size()];
        tasks.copyInto(futureArray);
        return joinResults(futureArray);
    }

    static Future<Result<PersistentList<PersistentMap<String, Object>>>> joinResults(
            Future<Result<PersistentList<PersistentMap<String, Object>>>> [] tasks) {
        return BareVertx.fastAll(tasks).map(composite -> {
            var flatList = Axiom.Data.<PersistentMap<String, Object>>emptyList().asTransient();

            for (var i = 0; i < composite.size(); i++) {
                Result<PersistentList<PersistentMap<String, Object>>> res = composite.resultAt(i);

                if (res.isFailure()) {
                    return Axiom.Check.<PersistentList<PersistentMap<String, Object>>>failure(
                            "Warp Breach [" + i + "]: " + res.failureValue().getMessage());
                }

                final var rowsFromStrike = res.getOrThrow();
                flatList = rowsFromStrike.fold(flatList, (acc, row) -> acc.append(row));
            }
            return Axiom.Check.success(flatList.freeze());
        }).recover(err -> BareVertx.fastSuccess(
                Axiom.Check.failure("Execution Scope Breach: " + err.getMessage())));
    }

    // 4. Chain: Sequential processing
    static Future<Result<PersistentList<PersistentMap<String, Object>>>> chain(
            PersistentList<Future<Result<PersistentMap<String, Object>>>> tasks) {
        return tasks.fold(//
                BareVertx.fastSuccess(Axiom.Check.success(Axiom.Data.emptyList())),
                (accFuture, taskFuture) -> accFuture.compose(accRes ->
                        accRes.isFailure()//
                                ? BareVertx.fastSuccess(accRes)//
                                : taskFuture.map(taskRes ->//
                                taskRes.flatMap(row ->//
                                        Axiom.Check.success(accRes.getOrThrow().append(row))//
                                )
                        )
                )
        );
    }
}