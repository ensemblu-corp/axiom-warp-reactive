package com.ensemblu.axiom.reactive.engine.core;

import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.spec.database.contract.StrikeInstruction;
import com.ensemblu.axiom.spec.parser.SqlParser;
import io.vertx.core.Future;

public interface ExecutionEngine<T> {
    Future<Result<T>> bindAndExecute(SqlParser.ExecutionPlan plan, StrikeInstruction instr, String translatedSql);
}