package com.ensemblu.axiom.reactive.engine.core;

import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.spec.database.contract.StrikeInstruction;
import com.ensemblu.axiom.spec.database.integrity.IngressIntegrity;
import com.ensemblu.axiom.spec.parser.SqlParser;
import io.vertx.core.Future;

public interface SovereignGate {
    static <T> Future<Result<T>> execute(//
            StrikeInstruction instruction,//
            SqlParser.ExecutionPlan plan,//
            String translatedSql,//
            ExecutionEngine<T> engine//
    ) {
        IngressIntegrity.verifyAlignment(plan, instruction.types(), instruction.data());

        return engine.bindAndExecute(plan, instruction, translatedSql);
    }
}