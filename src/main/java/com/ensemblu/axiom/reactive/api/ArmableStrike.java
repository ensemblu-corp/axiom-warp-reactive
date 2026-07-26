package com.ensemblu.axiom.reactive.api;

import com.ensemblu.axiom.core.data_structure.list.PersistentList;
import com.ensemblu.axiom.core.data_structure.map.PersistentMap;
import com.ensemblu.axiom.core.validation.Result;
import io.vertx.core.Future;
import io.vertx.sqlclient.SqlClient;

@FunctionalInterface
public interface ArmableStrike {
        Future<Result<PersistentList<PersistentMap<String, Object>>>> arm(SqlClient client);
}
