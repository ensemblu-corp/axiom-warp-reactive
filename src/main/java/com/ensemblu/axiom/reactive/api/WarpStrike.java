package com.ensemblu.axiom.reactive.api;

import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.spec.database.contract.StrikeInstruction;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;

import java.util.function.Function;


public  interface WarpStrike {

    <T> Future<Result<T>> read(Function<SqlClient, Future<Result<T>>> action);

    <T> Future<Result<T>>  write(Function<SqlClient, Future<Result<T>>> businessLogic);


     Future<Result<PersistentList<PersistentMap<String, Object>>>> parallelRead(
            PersistentList<StrikeInstruction> instructions) ;

     Future<Result<PersistentList<PersistentMap<String, Object>>>> parallelWrite(
            PersistentList<StrikeInstruction> instructions);
}