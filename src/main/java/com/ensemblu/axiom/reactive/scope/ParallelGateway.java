package com.ensemblu.axiom.reactive.scope;

import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.validation.Result;
import com.ensemblu.axiom.reactive.api.ArmableStrike;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;

@FunctionalInterface
public interface ParallelGateway {
    Future<Result<PersistentList<PersistentMap<String, Object>>>> apply(//
            SqlClient conn,//
            ArmableStrike strike//
    );
}